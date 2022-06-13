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

package org.apache.pulsar.io.kinesis;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Base64.getEncoder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.base.JacksonJsonValue;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.gson.JsonObject;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.kinesis.fbs.EncryptionCtx;
import org.apache.pulsar.io.kinesis.fbs.EncryptionKey;
import org.apache.pulsar.io.kinesis.fbs.KeyValue;
import org.apache.pulsar.io.kinesis.fbs.Message;
import org.apache.pulsar.io.kinesis.json.JsonConverter;
import org.apache.pulsar.io.kinesis.json.JsonRecord;

public class Utils {

    private static final String PAYLOAD_FIELD = "payloadBase64";
    private static final String PROPERTIES_FIELD = "properties";
    private static final String KEY_MAP_FIELD = "keysMapBase64";
    private static final String KEY_METADATA_MAP_FIELD = "keysMetadataMap";
    private static final String ENCRYPTION_PARAM_FIELD = "encParamBase64";
    private static final String ALGO_FIELD = "algorithm";
    private static final String COMPRESSION_TYPE_FIELD = "compressionType";
    private static final String UNCPRESSED_MSG_SIZE_FIELD = "uncompressedMessageSize";
    private static final String BATCH_SIZE_FIELD = "batchSize";
    private static final String ENCRYPTION_CTX_FIELD = "encryptionCtx";

    private static final FlatBufferBuilder DEFAULT_FB_BUILDER = new FlatBufferBuilder(0);

    /**
     * Serialize record to flat-buffer. it's not a thread-safe method.
     *
     * @param record the record to serialize
     * @return the buffer containing the serialized record
     */
    public static ByteBuffer serializeRecordToFlatBuffer(Record<GenericObject> record) {
        DEFAULT_FB_BUILDER.clear();
        return serializeRecordToFlatBuffer(DEFAULT_FB_BUILDER, record);
    }

    public static ByteBuffer serializeRecordToFlatBuffer(FlatBufferBuilder builder, Record<GenericObject> record) {
        checkNotNull(record, "record-context can't be null");
        final org.apache.pulsar.client.api.Message<GenericObject> recordMessage = getMessage(record);
        final Optional<EncryptionContext> encryptionCtx = recordMessage.getEncryptionCtx();
        final Map<String, String> properties = record.getProperties();

        int encryptionCtxOffset = -1;
        int propertiesOffset = -1;

        if (properties != null && !properties.isEmpty()) {
            int[] propertiesOffsetArray = new int[properties.size()];
            int i = 0;
            for (Entry<String, String> property : properties.entrySet()) {
                propertiesOffsetArray[i++] = KeyValue.createKeyValue(builder, builder.createString(property.getKey()),
                        builder.createString(property.getValue()));
            }
            propertiesOffset = Message.createPropertiesVector(builder, propertiesOffsetArray);
        }

        if (encryptionCtx.isPresent()) {
            encryptionCtxOffset = createEncryptionCtxOffset(builder, encryptionCtx.get());
        }

        int payloadOffset = Message.createPayloadVector(builder, recordMessage.getData());
        Message.startMessage(builder);
        Message.addPayload(builder, payloadOffset);
        if (encryptionCtxOffset != -1) {
            Message.addEncryptionCtx(builder, encryptionCtxOffset);
        }
        if (propertiesOffset != -1) {
            Message.addProperties(builder, propertiesOffset);
        }
        int endMessage = Message.endMessage(builder);
        builder.finish(endMessage);
        ByteBuffer bb = builder.dataBuffer();

        // to avoid copying of data, use same byte[] wrapped by ByteBuffer. But, ByteBuffer.array() returns entire array
        // so, it requires to read from offset:
        // builder.sizedByteArray()=>copies buffer: sizedByteArray(space, bb.capacity() - space)
        int space = bb.capacity() - builder.offset();
        return ByteBuffer.wrap(bb.array(), space, bb.capacity() - space);
    }

    private static int createEncryptionCtxOffset(final FlatBufferBuilder builder,
                                                 EncryptionContext ctx) {
        if (ctx == null) {
            return -1;
        }
        int[] keysOffsets = new int[ctx.getKeys().size()];
        int keyIndex = 0;
        for (Entry<String, org.apache.pulsar.common.api.EncryptionContext.EncryptionKey> entry : ctx.getKeys()
                .entrySet()) {
            int key = builder.createString(entry.getKey());
            int value = EncryptionKey.createValueVector(builder, entry.getValue().getKeyValue());
            Map<String, String> metadata = entry.getValue().getMetadata();
            int[] metadataOffsets = new int[metadata.size()];
            int i = 0;
            for (Entry<String, String> m : metadata.entrySet()) {
                metadataOffsets[i++] = KeyValue.createKeyValue(builder, builder.createString(m.getKey()),
                        builder.createString(m.getValue()));
            }
            int metadataOffset = -1;
            if (metadata.size() > 0) {
                metadataOffset = EncryptionKey.createMetadataVector(builder, metadataOffsets);
            }
            EncryptionKey.startEncryptionKey(builder);
            EncryptionKey.addKey(builder, key);
            EncryptionKey.addValue(builder, value);
            if (metadataOffset != -1) {
                EncryptionKey.addMetadata(builder, metadataOffset);
            }
            keysOffsets[keyIndex++] = EncryptionKey.endEncryptionKey(builder);
        }

        int keysOffset = EncryptionCtx.createKeysVector(builder, keysOffsets);
        int param = EncryptionCtx.createParamVector(builder, ctx.getParam());
        int algo = builder.createString(ctx.getAlgorithm());
        int batchSize = ctx.getBatchSize().isPresent() ? ctx.getBatchSize().get() : 1;
        byte compressionType;
        switch (ctx.getCompressionType()) {
            case LZ4:
                compressionType = org.apache.pulsar.io.kinesis.fbs.CompressionType.LZ4;
                break;
        case ZLIB:
            compressionType = org.apache.pulsar.io.kinesis.fbs.CompressionType.ZLIB;
            break;
        default:
            compressionType = org.apache.pulsar.io.kinesis.fbs.CompressionType.NONE;

        }
        return EncryptionCtx.createEncryptionCtx(builder, keysOffset, param, algo, compressionType,
                ctx.getUncompressedMessageSize(), batchSize, ctx.getBatchSize().isPresent());

    }

    /**
     * Serializes sink-record into json format. It encodes encryption-keys, encryption-param and payload in base64
     * format so, it can be sent in json.
     *
     * @param record the record to serialize
     * @return the record serialized to JSON
     */
    public static String serializeRecordToJson(Record<GenericObject> record) {
        checkNotNull(record, "record can't be null");
        final org.apache.pulsar.client.api.Message<GenericObject> recordMessage = getMessage(record);

        JsonObject result = new JsonObject();
        result.addProperty(PAYLOAD_FIELD, getEncoder().encodeToString(recordMessage.getData()));
        if (record.getProperties() != null) {
            JsonObject properties = new JsonObject();
            record.getProperties().forEach(properties::addProperty);
            result.add(PROPERTIES_FIELD, properties);
        }

        final Optional<EncryptionContext> optEncryptionCtx = recordMessage.getEncryptionCtx();
        if (optEncryptionCtx.isPresent()) {
            EncryptionContext encryptionCtx = optEncryptionCtx.get();
            JsonObject encryptionCtxJson = new JsonObject();
            JsonObject keyBase64Map = new JsonObject();
            JsonObject keyMetadataMap = new JsonObject();
            encryptionCtx.getKeys().forEach((key, value) -> {
                keyBase64Map.addProperty(key, getEncoder().encodeToString(value.getKeyValue()));
                Map<String, String> keyMetadata = value.getMetadata();
                if (keyMetadata != null && !keyMetadata.isEmpty()) {
                    JsonObject metadata = new JsonObject();
                    value.getMetadata().forEach(metadata::addProperty);
                    keyMetadataMap.add(key, metadata);
                }
            });
            encryptionCtxJson.add(KEY_MAP_FIELD, keyBase64Map);
            encryptionCtxJson.add(KEY_METADATA_MAP_FIELD, keyMetadataMap);
            encryptionCtxJson.addProperty(ENCRYPTION_PARAM_FIELD,
                    getEncoder().encodeToString(encryptionCtx.getParam()));
            encryptionCtxJson.addProperty(ALGO_FIELD, encryptionCtx.getAlgorithm());
            if (encryptionCtx.getCompressionType() != null) {
                encryptionCtxJson.addProperty(COMPRESSION_TYPE_FIELD, encryptionCtx.getCompressionType().name());
                encryptionCtxJson.addProperty(UNCPRESSED_MSG_SIZE_FIELD, encryptionCtx.getUncompressedMessageSize());
            }
            if (encryptionCtx.getBatchSize().isPresent()) {
                encryptionCtxJson.addProperty(BATCH_SIZE_FIELD, encryptionCtx.getBatchSize().get());
            }
            result.add(ENCRYPTION_CTX_FIELD, encryptionCtxJson);
        }
        return result.toString();
    }

    public static String serializeRecordToJsonExpandingValue(ObjectMapper mapper, Record<GenericObject> record,
                                                             boolean flatten)
            throws JsonProcessingException {
        JsonRecord jsonRecord = new JsonRecord();
        GenericObject value = record.getValue();
        if (value != null) {
            jsonRecord.setPayload(toJsonSerializable(record.getSchema(), value.getNativeObject()));
        }
        record.getKey().ifPresent(jsonRecord::setKey);
        record.getTopicName().ifPresent(jsonRecord::setTopicName);
        record.getEventTime().ifPresent(jsonRecord::setEventTime);
        record.getProperties().forEach(jsonRecord::addProperty);

        if (flatten) {
            JsonNode jsonNode = mapper.convertValue(jsonRecord, JsonNode.class);
            return JsonFlattener.flatten(new JacksonJsonValue(jsonNode));
        } else {
            return mapper.writeValueAsString(jsonRecord);
        }
    }

    public static org.apache.pulsar.client.api.Message<GenericObject> getMessage(Record<GenericObject> record) {
        return record.getMessage()
                .orElseThrow(() -> new IllegalArgumentException("Record does not carry message information"));
    }

    private static Object toJsonSerializable(Schema<?> schema, Object val) {
        if (schema == null || schema.getSchemaInfo().getType().isPrimitive()) {
            return val;
        }
        switch (schema.getSchemaInfo().getType()) {
            case KEY_VALUE:
                KeyValueSchema<GenericObject, GenericObject> keyValueSchema = (KeyValueSchema) schema;
                org.apache.pulsar.common.schema.KeyValue<GenericObject, GenericObject> keyValue =
                        (org.apache.pulsar.common.schema.KeyValue<GenericObject, GenericObject>) val;
                Map<String, Object> jsonKeyValue = new HashMap<>();
                jsonKeyValue.put("key", toJsonSerializable(keyValueSchema.getKeySchema(),
                        keyValue.getKey().getNativeObject()));
                jsonKeyValue.put("value", toJsonSerializable(keyValueSchema.getValueSchema(),
                        keyValue.getValue().getNativeObject()));
                return jsonKeyValue;
            case AVRO:
                return JsonConverter.toJson((org.apache.avro.generic.GenericRecord) val);
            case JSON:
                return val;
            default:
                throw new UnsupportedOperationException("Unsupported key schemaType="
                        + schema.getSchemaInfo().getType());
        }
    }

}