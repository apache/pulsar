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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.gson.Gson;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.RecordWithEncryptionContext;
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
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test
    public void testJsonSerialization() throws Exception {

        final String[] keyNames = { "key1", "key2" };
        final String key1Value = "test1";
        final String key2Value = "test2";
        final byte[][] keyValues = { key1Value.getBytes(), key2Value.getBytes() };
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
        Record<byte[]> recordCtx = createRecord(data, algo, keyNames, keyValues, param.getBytes(), metadata1, metadata2,
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

    @Test(dataProvider="encryption")
    public void testFbSerialization(boolean isEncryption) throws Exception {

        final String[] keyNames = { "key1", "key2" };
        final String param = "param";
        final String algo = "algo";
        int batchSize = 10;
        int compressionMsgSize = 10;

        for (int k = 0; k < 5; k++) {
            String payloadString = RandomStringUtils.random(142342 * k, String.valueOf(System.currentTimeMillis()));
            final String key1Value = payloadString + "test1";
            final String key2Value = payloadString + "test2";
            final byte[][] keyValues = { key1Value.getBytes(), key2Value.getBytes() };
            byte[] data = payloadString.getBytes();
            Map<String, String> properties = Maps.newHashMap();
            properties.put("prop1", payloadString);
            Map<String, String> metadata1 = Maps.newHashMap();
            metadata1.put("version", "v1");
            metadata1.put("ckms", "cmks-1");
            Map<String, String> metadata2 = Maps.newHashMap();
            metadata2.put("version", "v2");
            metadata2.put("ckms", "cmks-2");
            Record<byte[]> record = createRecord(data, algo, keyNames, keyValues, param.getBytes(), metadata1,
                    metadata2, batchSize, compressionMsgSize, properties, isEncryption);
            ByteBuffer flatBuffer = Utils.serializeRecordToFlatBuffer(record);

            Message kinesisJsonResponse = Message.getRootAsMessage(flatBuffer);
            byte[] fbPayloadBytes = new byte[kinesisJsonResponse.payloadLength()];
            kinesisJsonResponse.payloadAsByteBuffer().get(fbPayloadBytes);
            assertEquals(data, fbPayloadBytes);

            if(isEncryption) {
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

    private Record<byte[]> createRecord(byte[] data, String algo, String[] keyNames, byte[][] keyValues, byte[] param,
            Map<String, String> metadata1, Map<String, String> metadata2, int batchSize, int compressionMsgSize,
            Map<String, String> properties, boolean isEncryption) {
        EncryptionContext ctx = null;
        if(isEncryption) {
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
        return new RecordImpl(data, properties, Optional.ofNullable(ctx));
    }

    class RecordImpl implements RecordWithEncryptionContext<byte[]> {
        byte[] data;
        Map<String, String> properties;
        Optional<EncryptionContext> ectx;

        public RecordImpl(byte[] data, Map<String, String> properties, Optional<EncryptionContext> ectx) {
            this.data = data;
            this.properties = properties;
            this.ectx = ectx;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public Optional<EncryptionContext> getEncryptionCtx() {
            return ectx;
        }

        @Override
        public Optional<String> getKey() {
            return Optional.empty();
        }

        @Override
        public byte[] getValue() {
            return data;
        }
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

}
