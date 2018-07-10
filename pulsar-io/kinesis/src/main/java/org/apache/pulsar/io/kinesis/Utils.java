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

import static java.util.Base64.getEncoder;

import java.util.Map;

import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.io.core.RecordContext;

import com.google.gson.JsonObject;

public class Utils {

    private static final String PAYLOAD_FIELD = "payloadBase64";
    private static final String PROPERTIES_FIELD = "properties";
    private static final String KEY_MAP_FIELD = "keysMapBase64";
    private static final String KEY_METADATA_MAP_FIELD = "keysMetadataMap";
    private static final String METADATA_FIELD = "metadata";
    private static final String ENCRYPTION_PARAM_FIELD = "encParamBase64";
    private static final String ALGO_FIELD = "algorithm";
    private static final String COMPRESSION_TYPE_FIELD = "compressionType";
    private static final String UNCPRESSED_MSG_SIZE_FIELD = "uncompressedMessageSize";
    private static final String BATCH_SIZE_FIELD = "batchSize";
    private static final String ENCRYPTION_CTX_FIELD = "encryptionCtx";

    /**
     * Serializes sink-record into json format. It encodes encryption-keys, encryption-param and payload in base64
     * format so, it can be sent in json.
     * 
     * @param inputRecordContext
     * @param data
     * @return
     */
    public static String serializeRecordToJson(RecordContext inputRecordContext, byte[] data) {
        if (inputRecordContext == null) {
            return null;
        }
        JsonObject result = new JsonObject();
        result.addProperty(PAYLOAD_FIELD, getEncoder().encodeToString(data));
        if (inputRecordContext.getProperties() != null) {
            JsonObject properties = new JsonObject();
            inputRecordContext.getProperties().entrySet()
                    .forEach(e -> properties.addProperty(e.getKey(), e.getValue()));
            result.add(PROPERTIES_FIELD, properties);
        }
        if (inputRecordContext.getEncryptionCtx().isPresent()) {
            EncryptionContext encryptionCtx = inputRecordContext.getEncryptionCtx().get();
            JsonObject encryptionCtxJson = new JsonObject();
            JsonObject keyBase64Map = new JsonObject();
            JsonObject keyMetadataMap = new JsonObject();
            encryptionCtx.getKeys().entrySet().forEach(entry -> {
                keyBase64Map.addProperty(entry.getKey(), getEncoder().encodeToString(entry.getValue().getKeyValue()));
                Map<String, String> keyMetadata = entry.getValue().getMetadata();
                if (keyMetadata != null && !keyMetadata.isEmpty()) {
                    JsonObject metadata = new JsonObject();
                    entry.getValue().getMetadata().entrySet()
                            .forEach(m -> metadata.addProperty(m.getKey(), m.getValue()));
                    keyMetadataMap.add(entry.getKey(), metadata);
                }
            });
            encryptionCtxJson.add(KEY_MAP_FIELD, keyBase64Map);
            encryptionCtxJson.add(KEY_METADATA_MAP_FIELD, keyMetadataMap);
            Map<String, String> metadataMap = encryptionCtx.getMetadata();
            if (metadataMap != null && !metadataMap.isEmpty()) {
                JsonObject metadata = new JsonObject();
                encryptionCtx.getMetadata().entrySet().forEach(m -> metadata.addProperty(m.getKey(), m.getValue()));
                encryptionCtxJson.add(METADATA_FIELD, metadata);
            }
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

}