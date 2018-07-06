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

import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.apache.pulsar.io.core.RecordContext;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import com.google.gson.Gson;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Unit test of {@link UtilsTest}.
 */
public class UtilsTest {

    @Test
    public void testJsonSerialization() throws Exception {

        final String key1 = "key1";
        final String key2 = "key2";
        final String key1Value = "test1";
        final String key2Value = "test2";
        final String param = "param";
        final String algo = "algo";

        // prepare encryption-ctx
        EncryptionContext ctx = new EncryptionContext();
        ctx.setAlgorithm(algo);
        ctx.setBatchSize(Optional.of(10));
        ctx.setCompressionType(CompressionType.LZ4);
        ctx.setUncompressedMessageSize(10);
        Map<String, EncryptionKey> keys = Maps.newHashMap();
        EncryptionKey encKeyVal = new EncryptionKey();
        encKeyVal.setKeyValue(key1Value.getBytes());
        Map<String, String> metadata1 = Maps.newHashMap();
        metadata1.put("version", "v1");
        metadata1.put("ckms", "cmks-1");
        encKeyVal.setMetadata(metadata1);
        EncryptionKey encKeyVal2 = new EncryptionKey();
        encKeyVal2.setKeyValue(key2Value.getBytes());
        Map<String, String> metadata2 = Maps.newHashMap();
        metadata2.put("version", "v2");
        metadata2.put("ckms", "cmks-2");
        encKeyVal2.setMetadata(metadata2);
        keys.put(key1, encKeyVal);
        keys.put(key2, encKeyVal2);
        ctx.setKeys(keys);
        ctx.setMetadata(metadata1);
        ctx.setParam(param.getBytes());

        // serialize to json
        byte[] data = "payload".getBytes();
        Map<String, String> properties = Maps.newHashMap();
        properties.put("prop1", "value");
        RecordContext recordCtx = new RecordContextImpl(properties, ctx);
        String json = Utils.serializeRecordToJson(recordCtx, data);
        System.out.println(json);

        // deserialize from json and assert
        KinesisMessageResponse kinesisJsonResponse = deSerializeRecordFromJson(json);
        Assert.assertEquals(data, getDecoder().decode(kinesisJsonResponse.getPayloadBase64()));
        EncryptionCtx encryptionCtxDeser = kinesisJsonResponse.getEncryptionCtx();
        Assert.assertEquals(key1Value.getBytes(), getDecoder().decode(encryptionCtxDeser.getKeysMapBase64().get(key1)));
        Assert.assertEquals(key2Value.getBytes(), getDecoder().decode(encryptionCtxDeser.getKeysMapBase64().get(key2)));
        Assert.assertEquals(param.getBytes(), getDecoder().decode(encryptionCtxDeser.getEncParamBase64()));
        Assert.assertEquals(algo, encryptionCtxDeser.getAlgorithm());
        Assert.assertEquals(metadata1, encryptionCtxDeser.getKeysMetadataMap().get(key1));
        Assert.assertEquals(metadata2, encryptionCtxDeser.getKeysMetadataMap().get(key2));
        Assert.assertEquals(metadata1, encryptionCtxDeser.getMetadata());
        Assert.assertEquals(properties, kinesisJsonResponse.getProperties());

    }

    class RecordContextImpl implements RecordContext {
        Map<String, String> properties;
        Optional<EncryptionContext> ectx;

        public RecordContextImpl(Map<String, String> properties, EncryptionContext ectx) {
            this.properties = properties;
            this.ectx = Optional.of(ectx);
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public Optional<EncryptionContext> getEncryptionCtx() {
            return ectx;
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
        // encryption-ctx metadata
        private Map<String, String> metadata;
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
