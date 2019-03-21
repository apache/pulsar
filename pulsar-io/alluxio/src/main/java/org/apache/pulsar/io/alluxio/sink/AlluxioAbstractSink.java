/**
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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A Simple abstract class for Alluxio sink
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class AlluxioAbstractSink<K, V> implements Sink<V> {

    private FileSystem fileSystem;
    private FileOutStream fileOutStream;
    private CreateFileOptions createFileOptions;
    private long recordsNum;
    private String tmpFilePath;
    private String fileDirPath;
    private String tmpFileDirPath;
    private long lastRotationTime;
    private long rotationRecordsNum;
    private long rotationInterval;
    private AlluxioSinkConfig alluxioSinkConfig;
    private AlluxioState alluxioState;

    private List<Record<V>> recordsToAck;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        alluxioSinkConfig = AlluxioSinkConfig.load(config);
        alluxioSinkConfig.validate();

        // initialize FileSystem
        String alluxioMasterHost = alluxioSinkConfig.getAlluxioMasterHost();
        int alluxioMasterPort = alluxioSinkConfig.getAlluxioMasterPort();
        Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioMasterHost);
        Configuration.set(PropertyKey.MASTER_RPC_PORT, alluxioMasterPort);
        if (alluxioSinkConfig.getSecurityLoginUser() != null) {
            Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, alluxioSinkConfig.getSecurityLoginUser());
        }
        fileSystem = FileSystem.Factory.get();

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

        createFileOptions = CreateFileOptions.defaults();

        recordsNum = 0;
        recordsToAck = Lists.newArrayList();
        tmpFilePath = "";
        alluxioState = AlluxioState.WRITE_STARTED;

        lastRotationTime = System.currentTimeMillis();
        rotationRecordsNum = alluxioSinkConfig.getRotationRecords();
        rotationInterval =  alluxioSinkConfig.getRotationInterval();
    }

    @Override
    public void write(Record<V> record) {
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

    private void writeToAlluxio(Record<V> record) throws AlluxioException, IOException {
        KeyValue<K, V> keyValue = extractKeyValue(record);
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
            WriteType writeType;
            try {
                writeType = WriteType.valueOf(alluxioSinkConfig.getWriteType().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Illegal write type when creating Alluxio files, valid values are: "
                    + Arrays.asList(WriteType.values()));
            }
            createFileOptions.setWriteType(writeType);
        }
        fileOutStream = fileSystem.createFile(new AlluxioURI(tmpFilePath), createFileOptions);
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

    private byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            oos.flush();
            bytes = baos.toByteArray();
        } catch (IOException e) {
            log.error("Failed to serialize the object.", e);
        }
        return bytes;
    }

    private byte[] toBytes(Object obj) {
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

    public abstract KeyValue<K, V> extractKeyValue(Record<V> record);

    private enum AlluxioState {
        WRITE_STARTED,
        FILE_ROTATED,
        FILE_COMMITTED
    }
}
