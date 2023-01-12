/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.alluxio.sink;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.util.FileSystemOptions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

/**
 * Alluxio sink that treats incoming messages on the input topic as Strings
 * and write identical key/value pairs.
 */
@Connector(
        name = "alluxio",
        type = IOType.SINK,
        help = "The sink connector is used for moving records from Pulsar to Alluxio.",
        configClass = AlluxioSinkConfig.class)
@Slf4j
public class AlluxioSink implements Sink<GenericObject> {

    private FileSystem fileSystem;
    private FileOutStream fileOutStream;
    private CreateFilePOptions.Builder optionsBuilder;
    private long recordsNum;
    private String tmpFilePath;
    private String fileDirPath;
    private String tmpFileDirPath;
    private long lastRotationTime;
    private long rotationRecordsNum;
    private long rotationInterval;
    private AlluxioSinkConfig alluxioSinkConfig;
    private AlluxioState alluxioState;

    private InstancedConfiguration configuration = InstancedConfiguration.defaults();

    private ObjectMapper objectMapper = new ObjectMapper();

    private List<Record<GenericObject>> recordsToAck;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        alluxioSinkConfig = AlluxioSinkConfig.load(config);
        alluxioSinkConfig.validate();

        // initialize FileSystem
        String alluxioMasterHost = alluxioSinkConfig.getAlluxioMasterHost();
        int alluxioMasterPort = alluxioSinkConfig.getAlluxioMasterPort();
        configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioMasterHost);
        configuration.set(PropertyKey.MASTER_RPC_PORT, alluxioMasterPort);
        if (alluxioSinkConfig.getSecurityLoginUser() != null) {
            configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, alluxioSinkConfig.getSecurityLoginUser());
        }
        fileSystem = FileSystem.Factory.create(configuration);

        // initialize alluxio dirs
        String alluxioDir = alluxioSinkConfig.getAlluxioDir();
        fileDirPath = alluxioDir.startsWith("/") ? alluxioDir : "/" + alluxioDir;
        tmpFileDirPath = fileDirPath + "/tmp";

        AlluxioURI alluxioDirPath = new AlluxioURI(fileDirPath);
        if (!fileSystem.exists(alluxioDirPath)) {
            fileSystem.createDirectory(alluxioDirPath);
        }

        AlluxioURI tmpAlluxioDirPath = new AlluxioURI(tmpFileDirPath);
        if (!fileSystem.exists(tmpAlluxioDirPath)) {
            fileSystem.createDirectory(tmpAlluxioDirPath);
        }

        optionsBuilder = FileSystemOptions.createFileDefaults(configuration).toBuilder();

        recordsNum = 0;
        recordsToAck = Lists.newArrayList();
        tmpFilePath = "";
        alluxioState = AlluxioState.WRITE_STARTED;

        lastRotationTime = System.currentTimeMillis();
        rotationRecordsNum = alluxioSinkConfig.getRotationRecords();
        rotationInterval =  alluxioSinkConfig.getRotationInterval();
    }

    @SuppressWarnings("checkstyle:fallthrough")
    @Override
    public void write(Record<GenericObject> record) {
        long now = System.currentTimeMillis();

        switch (alluxioState) {
            case WRITE_STARTED:
                try {
                    writeToAlluxio(record);
                    if (!shouldRotate(now)) {
                        break;
                    }
                    alluxioState = AlluxioState.FILE_ROTATED;
                } catch (AlluxioException | IOException e) {
                    log.error("Unable to write record to alluxio.", e);
                    record.fail();
                    break;
                }
            case FILE_ROTATED:
                try {
                    closeAndCommitTmpFile();
                    alluxioState = AlluxioState.FILE_COMMITTED;
                    ackRecords();
                } catch (AlluxioException | IOException e) {
                    log.error("Unable to flush records to alluxio.", e);
                    failRecords();
                    try {
                        deleteTmpFile();
                    } catch (AlluxioException | IOException e1) {
                        log.error("Failed to delete tmp cache file.", e);
                    }
                    break;
                }
            case FILE_COMMITTED:
                alluxioState = AlluxioState.WRITE_STARTED;
                break;
            default:
                log.error("{} is not a valid state when writing record to alluxio temp dir {}.",
                    alluxioState, tmpFileDirPath);
                break;
        }
    }

    @Override
    public void close() throws Exception {
        // flush records in the tmpFile when closing sink
        try {
            closeAndCommitTmpFile();
            ackRecords();
        } catch (AlluxioException | IOException e) {
            log.error("Unable to flush records to alluxio.", e);
            failRecords();
        }
        deleteTmpFile();
    }

    private void ackRecords() {
        recordsToAck.forEach(Record::ack);
        recordsToAck.clear();
    }

    private void failRecords() {
        recordsToAck.forEach(Record::fail);
        recordsToAck.clear();
    }

    private void writeToAlluxio(Record<GenericObject> record) throws AlluxioException, IOException {
        KeyValue<String, String> keyValue = extractKeyValue(record);
        if (fileOutStream == null) {
            createTmpFile();
        }
        fileOutStream.write(toBytes(keyValue.getValue()));
        if (alluxioSinkConfig.getLineSeparator() != '\u0000') {
            fileOutStream.write(alluxioSinkConfig.getLineSeparator());
        }
        recordsNum++;
        recordsToAck.add(record);
    }

    private void createTmpFile() throws AlluxioException, IOException {
        UUID id = UUID.randomUUID();
        String fileExtension = alluxioSinkConfig.getFileExtension();
        tmpFilePath = tmpFileDirPath + "/" + id.toString() + "_tmp" + fileExtension;
        if (alluxioSinkConfig.getWriteType() != null) {
            WritePType writePType;
            try {
                writePType = WritePType.valueOf(alluxioSinkConfig.getWriteType().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Illegal write type when creating Alluxio files, valid values are: "
                    + Arrays.asList(WriteType.values()));
            }
            optionsBuilder.setWriteType(writePType);
        }
        fileOutStream = fileSystem.createFile(new AlluxioURI(tmpFilePath), optionsBuilder.build());
    }

    private void closeAndCommitTmpFile() throws AlluxioException, IOException {
        // close the tmpFile
        if (fileOutStream != null) {
            fileOutStream.close();
        }
        // commit the tmpFile
        String filePrefix = alluxioSinkConfig.getFilePrefix();
        String fileExtension = alluxioSinkConfig.getFileExtension();
        String newFile = filePrefix + "-" + System.currentTimeMillis() + fileExtension;
        String newFilePath = fileDirPath + "/" + newFile;
        fileSystem.rename(new AlluxioURI(tmpFilePath), new AlluxioURI(newFilePath));
        fileOutStream = null;
        tmpFilePath = "";
        recordsNum = 0;
        lastRotationTime = System.currentTimeMillis();
    }

    private void deleteTmpFile() throws AlluxioException, IOException {
        if (!tmpFilePath.equals("")) {
            fileSystem.delete(new AlluxioURI(tmpFilePath));
        }
    }

    private boolean shouldRotate(long now) {
        boolean rotated = false;
        if (recordsNum >= rotationRecordsNum) {
            rotated = true;
        } else {
            if (rotationInterval != -1 && (now - lastRotationTime) >= rotationInterval) {
                rotated = true;
            }
        }
        return rotated;
    }

    private static byte[] toByteArray(Object obj) throws IOException {
        byte[] bytes = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            oos.flush();
            bytes = baos.toByteArray();
        } catch (IOException e) {
            log.error("Failed to serialize the object.", e);
            throw e;
        }
        return bytes;
    }

    private static byte[] toBytes(Object obj) throws IOException {
        byte[] bytes;
        if (obj instanceof String) {
            String s = (String) obj;
            bytes = s.getBytes(StandardCharsets.UTF_8);
        } else if (obj instanceof byte[]) {
            bytes = (byte[]) obj;
        } else {
            bytes = toByteArray(obj);
        }
        return bytes;
    }

    public KeyValue<String, String> extractKeyValue(Record<GenericObject> record) throws JsonProcessingException {
        // just ignore the key
        if (alluxioSinkConfig.isSchemaEnable()) {
            GenericObject recordValue = null;
            Schema<?> valueSchema = null;
            if (record.getSchema() != null && record.getSchema() instanceof KeyValueSchema) {
                KeyValueSchema<GenericObject, GenericObject> keyValueSchema = (KeyValueSchema) record.getSchema();
                valueSchema = keyValueSchema.getValueSchema();
                org.apache.pulsar.common.schema.KeyValue<GenericObject, GenericObject> keyValue =
                        (org.apache.pulsar.common.schema.KeyValue<GenericObject, GenericObject>)
                                record.getValue().getNativeObject();
                recordValue = keyValue.getValue();
            } else {
                valueSchema = record.getSchema();
                recordValue = record.getValue();
            }

            String value = null;
            if (recordValue != null) {
                if (valueSchema != null) {
                    value = stringifyValue(valueSchema, recordValue);
                } else {
                    if (recordValue.getNativeObject() instanceof byte[]) {
                        value = new String((byte[]) recordValue.getNativeObject(), StandardCharsets.UTF_8);
                    } else {
                        value = recordValue.getNativeObject().toString();
                    }
                }
            }
            return new KeyValue<>(null, value);
        } else {
            return new KeyValue<>(null, new String(record.getMessage()
                    .orElseThrow(() -> new IllegalArgumentException("Record does not carry message information"))
                    .getData(), StandardCharsets.UTF_8));
        }
    }

    public String stringifyValue(Schema<?> schema, Object val) throws JsonProcessingException {
        // just support json schema
        if (schema.getSchemaInfo().getType() == SchemaType.JSON) {
            JsonNode jsonNode = (JsonNode) ((GenericRecord) val).getNativeObject();
            return objectMapper.writeValueAsString(jsonNode);
        }
        throw new UnsupportedOperationException("Unsupported value schemaType=" + schema.getSchemaInfo().getType());
    }

    private enum AlluxioState {
        WRITE_STARTED,
        FILE_ROTATED,
        FILE_COMMITTED
    }
}
