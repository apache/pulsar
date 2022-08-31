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

import static java.util.Base64.getDecoder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.kinesis.fbs.KeyValue;
import org.apache.pulsar.io.kinesis.fbs.Message;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Unit test of {@link UtilsTest}.
 */
public class UtilsTest {

    @DataProvider(name = "encryption")
    public Object[][] encryptionProvider() {
        return new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}};
    }

    @Test
    public void testJsonSerialization() {

        final String[] keyNames = {"key1", "key2"};
        final String key1Value = "test1";
        final String key2Value = "test2";
        final byte[][] keyValues = {key1Value.getBytes(), key2Value.getBytes()};
        final String param = "param";
        final String algo = "algo";
        int batchSize = 10;
        int compressionMsgSize = 10;

        // serialize to json
        byte[] data = "payload".getBytes();
        Map<String, String> properties = Maps.newHashMap();
        properties.put("prop1", "value");
        Map<String, String> metadata1 = Maps.newHashMap();
        metadata1.put("version", "v1");
        metadata1.put("ckms", "cmks-1");
        Map<String, String> metadata2 = Maps.newHashMap();
        metadata2.put("version", "v2");
        metadata2.put("ckms", "cmks-2");
        Record<GenericObject> recordCtx =
                createRecord(data, algo, keyNames, keyValues, param.getBytes(), metadata1, metadata2,
                        batchSize, compressionMsgSize, properties, true);
        String json = Utils.serializeRecordToJson(recordCtx);

        // deserialize from json and assert
        KinesisMessageResponse kinesisJsonResponse = deSerializeRecordFromJson(json);
        assertEquals(data, getDecoder().decode(kinesisJsonResponse.getPayloadBase64()));
        EncryptionCtx encryptionCtxDeser = kinesisJsonResponse.getEncryptionCtx();
        assertEquals(key1Value.getBytes(),
                getDecoder().decode(encryptionCtxDeser.getKeysMapBase64().get(keyNames[0])));
        assertEquals(key2Value.getBytes(),
                getDecoder().decode(encryptionCtxDeser.getKeysMapBase64().get(keyNames[1])));
        assertEquals(param.getBytes(), getDecoder().decode(encryptionCtxDeser.getEncParamBase64()));
        assertEquals(algo, encryptionCtxDeser.getAlgorithm());
        assertEquals(metadata1, encryptionCtxDeser.getKeysMetadataMap().get(keyNames[0]));
        assertEquals(metadata2, encryptionCtxDeser.getKeysMetadataMap().get(keyNames[1]));
        assertEquals(properties, kinesisJsonResponse.getProperties());

    }

    @Test(dataProvider = "encryption")
    public void testFbSerialization(boolean isEncryption) {

        final String[] keyNames = {"key1", "key2"};
        final String param = "param";
        final String algo = "algo";
        int batchSize = 10;
        int compressionMsgSize = 10;

        for (int k = 0; k < 5; k++) {
            String payloadString = RandomStringUtils.random(142342 * k, String.valueOf(System.currentTimeMillis()));
            final String key1Value = payloadString + "test1";
            final String key2Value = payloadString + "test2";
            final byte[][] keyValues = {key1Value.getBytes(), key2Value.getBytes()};
            byte[] data = payloadString.getBytes();
            Map<String, String> properties = Maps.newHashMap();
            properties.put("prop1", payloadString);
            Map<String, String> metadata1 = Maps.newHashMap();
            metadata1.put("version", "v1");
            metadata1.put("ckms", "cmks-1");
            Map<String, String> metadata2 = Maps.newHashMap();
            metadata2.put("version", "v2");
            metadata2.put("ckms", "cmks-2");
            Record<GenericObject> record = createRecord(data, algo, keyNames, keyValues, param.getBytes(), metadata1,
                    metadata2, batchSize, compressionMsgSize, properties, isEncryption);
            ByteBuffer flatBuffer = Utils.serializeRecordToFlatBuffer(record);

            Message kinesisJsonResponse = Message.getRootAsMessage(flatBuffer);
            byte[] fbPayloadBytes = new byte[kinesisJsonResponse.payloadLength()];
            kinesisJsonResponse.payloadAsByteBuffer().get(fbPayloadBytes);
            assertEquals(data, fbPayloadBytes);

            if (isEncryption) {
                org.apache.pulsar.io.kinesis.fbs.EncryptionCtx encryptionCtxDeser = kinesisJsonResponse.encryptionCtx();
                byte compressionType = encryptionCtxDeser.compressionType();
                int fbBatchSize = encryptionCtxDeser.batchSize();
                boolean isBathcMessage = encryptionCtxDeser.isBatchMessage();
                int fbCompressionMsgSize = encryptionCtxDeser.uncompressedMessageSize();
                int totalKeys = encryptionCtxDeser.keysLength();
                Map<String, Map<String, String>> fbKeyMetadataResult = Maps.newHashMap();
                Map<String, byte[]> fbKeyValueResult = Maps.newHashMap();
                for (int i = 0; i < encryptionCtxDeser.keysLength(); i++) {
                    org.apache.pulsar.io.kinesis.fbs.EncryptionKey encryptionKey = encryptionCtxDeser.keys(i);
                    String keyName = encryptionKey.key();
                    byte[] keyValueBytes = new byte[encryptionKey.valueLength()];
                    encryptionKey.valueAsByteBuffer().get(keyValueBytes);
                    fbKeyValueResult.put(keyName, keyValueBytes);
                    Map<String, String> fbMetadata = Maps.newHashMap();
                    for (int j = 0; j < encryptionKey.metadataLength(); j++) {
                        KeyValue encMtdata = encryptionKey.metadata(j);
                        fbMetadata.put(encMtdata.key(), encMtdata.value());
                    }
                    fbKeyMetadataResult.put(keyName, fbMetadata);
                }
                byte[] paramBytes = new byte[encryptionCtxDeser.paramLength()];
                encryptionCtxDeser.paramAsByteBuffer().get(paramBytes);

                assertEquals(totalKeys, 2);
                assertEquals(batchSize, fbBatchSize);
                assertTrue(isBathcMessage);
                assertEquals(compressionMsgSize, fbCompressionMsgSize);
                assertEquals(keyValues[0], fbKeyValueResult.get(keyNames[0]));
                assertEquals(keyValues[1], fbKeyValueResult.get(keyNames[1]));
                assertEquals(metadata1, fbKeyMetadataResult.get(keyNames[0]));
                assertEquals(metadata2, fbKeyMetadataResult.get(keyNames[1]));
                assertEquals(compressionType, org.apache.pulsar.io.kinesis.fbs.CompressionType.LZ4);
                assertEquals(param.getBytes(), paramBytes);
                assertEquals(algo, encryptionCtxDeser.algo());
            }

            Map<String, String> fbproperties = Maps.newHashMap();
            for (int i = 0; i < kinesisJsonResponse.propertiesLength(); i++) {
                KeyValue property = kinesisJsonResponse.properties(i);
                fbproperties.put(property.key(), property.value());
            }
            assertEquals(properties, fbproperties);

        }
    }

    private Record<GenericObject> createRecord(byte[] data, String algo, String[] keyNames, byte[][] keyValues,
                                               byte[] param,
                                               Map<String, String> metadata1, Map<String, String> metadata2,
                                               int batchSize, int compressionMsgSize,
                                               Map<String, String> properties, boolean isEncryption) {
        EncryptionContext ctx = null;
        if (isEncryption) {
            ctx = new EncryptionContext();
            ctx.setAlgorithm(algo);
            ctx.setBatchSize(Optional.of(batchSize));
            ctx.setCompressionType(CompressionType.LZ4);
            ctx.setUncompressedMessageSize(compressionMsgSize);
            Map<String, EncryptionKey> keys = Maps.newHashMap();
            EncryptionKey encKeyVal = new EncryptionKey();
            encKeyVal.setKeyValue(keyValues[0]);

            encKeyVal.setMetadata(metadata1);
            EncryptionKey encKeyVal2 = new EncryptionKey();
            encKeyVal2.setKeyValue(keyValues[1]);
            encKeyVal2.setMetadata(metadata2);
            keys.put(keyNames[0], encKeyVal);
            keys.put(keyNames[1], encKeyVal2);
            ctx.setKeys(keys);
            ctx.setParam(param);
        }

        org.apache.pulsar.client.api.Message<GenericObject> message = mock(org.apache.pulsar.client.api.Message.class);
        when(message.getData()).thenReturn(data);
        when(message.getProperties()).thenReturn(properties);
        when(message.getEncryptionCtx()).thenReturn(Optional.ofNullable(ctx));

        return PulsarRecord.<GenericObject>builder().message(message).build();
    }

    public static KinesisMessageResponse deSerializeRecordFromJson(String jsonRecord) {
        if (StringUtils.isNotBlank(jsonRecord)) {
            return new Gson().fromJson(jsonRecord, KinesisMessageResponse.class);
        }
        return null;
    }

    @ToString
    @Setter
    @Getter
    public static class KinesisMessageResponse {
        // Encryption-context if message has been encrypted
        private EncryptionCtx encryptionCtx;
        // user-properties
        private Map<String, String> properties;
        // base64 encoded payload
        private String payloadBase64;
    }

    @ToString
    @Setter
    @Getter
    public static class EncryptionCtx {
        // map of encryption-key value. (key-value is base64 encoded)
        private Map<String, String> keysMapBase64;
        // map of encryption-key metadata
        private Map<String, Map<String, String>> keysMetadataMap;
        // encryption param which is base64 encoded
        private String encParamBase64;
        // encryption algorithm
        private String algorithm;
        // compression type if message is compressed
        private CompressionType compressionType;
        private int uncompressedMessageSize;
        // number of messages in the batch if msg is batched message
        private Integer batchSize;
    }

    @DataProvider(name = "schemaType")
    public Object[] schemaType() {
        return new Object[]{SchemaType.JSON, SchemaType.AVRO};
    }

    @Test(dataProvider = "schemaType")
    public void testSerializeRecordToJsonExpandingValue(SchemaType schemaType) throws Exception {
        RecordSchemaBuilder valueSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("value");
        valueSchemaBuilder.field("c").type(SchemaType.STRING).optional().defaultValue(null);
        valueSchemaBuilder.field("d").type(SchemaType.INT32).optional().defaultValue(null);
        RecordSchemaBuilder udtSchemaBuilder = SchemaBuilder.record("type1");
        udtSchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        udtSchemaBuilder.field("b").type(SchemaType.BOOLEAN).optional().defaultValue(null);
        udtSchemaBuilder.field("d").type(SchemaType.DOUBLE).optional().defaultValue(null);
        udtSchemaBuilder.field("f").type(SchemaType.FLOAT).optional().defaultValue(null);
        udtSchemaBuilder.field("i").type(SchemaType.INT32).optional().defaultValue(null);
        udtSchemaBuilder.field("l").type(SchemaType.INT64).optional().defaultValue(null);
        GenericSchema<GenericRecord> udtGenericSchema = Schema.generic(udtSchemaBuilder.build(schemaType));
        valueSchemaBuilder.field("e", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

        GenericRecord valueGenericRecord = valueSchema.newRecordBuilder()
                .set("c", "1")
                .set("d", 1)
                .set("e", udtGenericSchema.newRecordBuilder()
                        .set("a", "a")
                        .set("b", true)
                        .set("d", 1.0)
                        .set("f", 1.0f)
                        .set("i", 1)
                        .set("l", 10L)
                        .build())
                .build();

        Map<String, String> properties = new HashMap<>();
        properties.put("prop-key", "prop-value");

        Record<GenericObject> genericObjectRecord = new Record<GenericObject>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of("data-ks1.table1");
            }

            @Override
            public org.apache.pulsar.client.api.Schema getSchema() {
                return valueSchema;
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of("message-key");
            }

            @Override
            public GenericObject getValue() {
                return valueGenericRecord;
            }

            @Override
            public Map<String, String> getProperties() {
                return properties;
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(1648502845803L);
            }
        };

        ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String json = Utils.serializeRecordToJsonExpandingValue(objectMapper, genericObjectRecord, false);

        assertEquals(json, "{\"topicName\":\"data-ks1.table1\",\"key\":\"message-key\",\"payload\":{\"c\":\"1\","
                + "\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}},"
                + "\"properties\":{\"prop-key\":\"prop-value\"},\"eventTime\":1648502845803}");
    }

    @Test(dataProvider = "schemaType")
    public void testKeyValueSerializeRecordToJsonExpandingValue(SchemaType schemaType) throws Exception {
        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("key");
        keySchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        keySchemaBuilder.field("b").type(SchemaType.INT32).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(schemaType));
        GenericRecord keyGenericRecord = keySchema.newRecordBuilder()
                .set("a", "1")
                .set("b", 1)
                .build();

        RecordSchemaBuilder valueSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("value");
        valueSchemaBuilder.field("c").type(SchemaType.STRING).optional().defaultValue(null);
        valueSchemaBuilder.field("d").type(SchemaType.INT32).optional().defaultValue(null);
        RecordSchemaBuilder udtSchemaBuilder = SchemaBuilder.record("type1");
        udtSchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        udtSchemaBuilder.field("b").type(SchemaType.BOOLEAN).optional().defaultValue(null);
        udtSchemaBuilder.field("d").type(SchemaType.DOUBLE).optional().defaultValue(null);
        udtSchemaBuilder.field("f").type(SchemaType.FLOAT).optional().defaultValue(null);
        udtSchemaBuilder.field("i").type(SchemaType.INT32).optional().defaultValue(null);
        udtSchemaBuilder.field("l").type(SchemaType.INT64).optional().defaultValue(null);
        GenericSchema<GenericRecord> udtGenericSchema = Schema.generic(udtSchemaBuilder.build(schemaType));
        valueSchemaBuilder.field("e", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

        GenericRecord valueGenericRecord = valueSchema.newRecordBuilder()
                .set("c", "1")
                .set("d", 1)
                .set("e", udtGenericSchema.newRecordBuilder()
                        .set("a", "a")
                        .set("b", true)
                        .set("d", 1.0)
                        .set("f", 1.0f)
                        .set("i", 1)
                        .set("l", 10L)
                        .build())
                .build();

        Schema<org.apache.pulsar.common.schema.KeyValue<GenericRecord, GenericRecord>> keyValueSchema =
                Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.INLINE);
        org.apache.pulsar.common.schema.KeyValue<GenericRecord, GenericRecord>
                keyValue = new org.apache.pulsar.common.schema.KeyValue<>(keyGenericRecord, valueGenericRecord);
        GenericObject genericObject = new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.KEY_VALUE;
            }

            @Override
            public Object getNativeObject() {
                return keyValue;
            }
        };

        Map<String, String> properties = new HashMap<>();
        properties.put("prop-key", "prop-value");

        Record<GenericObject> genericObjectRecord = new Record<GenericObject>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of("data-ks1.table1");
            }

            @Override
            public org.apache.pulsar.client.api.Schema getSchema() {
                return keyValueSchema;
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of("message-key");
            }

            @Override
            public GenericObject getValue() {
                return genericObject;
            }

            @Override
            public Map<String, String> getProperties() {
                return properties;
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(1648502845803L);
            }
        };

        ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String json = Utils.serializeRecordToJsonExpandingValue(objectMapper, genericObjectRecord, false);

        assertEquals(json, "{\"topicName\":\"data-ks1.table1\",\"key\":\"message-key\","
                + "\"payload\":{\"value\":{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,"
                + "\"i\":1,\"l\":10}},\"key\":{\"a\":\"1\",\"b\":1}},\"properties\":{\"prop-key\":\"prop-value\"},"
                + "\"eventTime\":1648502845803}");

        json = Utils.serializeRecordToJsonExpandingValue(objectMapper, genericObjectRecord, true);

        assertEquals(json, "{\"topicName\":\"data-ks1.table1\",\"key\":\"message-key\",\"payload.value.c\":\"1\","
                + "\"payload.value.d\":1,\"payload.value.e.a\":\"a\",\"payload.value.e.b\":true,\"payload.value.e"
                + ".d\":1.0,\"payload.value.e.f\":1.0,\"payload.value.e.i\":1,\"payload.value.e.l\":10,\"payload.key"
                + ".a\":\"1\",\"payload.key.b\":1,\"properties.prop-key\":\"prop-value\",\"eventTime\":1648502845803}");
    }

    @Test
    public void testPrimitiveSerializeRecordToJsonExpandingValue() throws Exception {
        GenericObject genericObject = new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.STRING;
            }

            @Override
            public Object getNativeObject() {
                return "message-value";
            }
        };

        Map<String, String> properties = new HashMap<>();
        properties.put("prop-key", "prop-value");

        Record<GenericObject> genericObjectRecord = new Record<GenericObject>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of("data-ks1.table1");
            }

            @Override
            public org.apache.pulsar.client.api.Schema getSchema() {
                return Schema.STRING;
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of("message-key");
            }

            @Override
            public GenericObject getValue() {
                return genericObject;
            }

            @Override
            public Map<String, String> getProperties() {
                return properties;
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(1648502845803L);
            }
        };

        ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String json = Utils.serializeRecordToJsonExpandingValue(objectMapper, genericObjectRecord, false);

        assertEquals(json, "{\"topicName\":\"data-ks1.table1\",\"key\":\"message-key\",\"payload\":\"message-value\","
                + "\"properties\":{\"prop-key\":\"prop-value\"},\"eventTime\":1648502845803}");
    }
}