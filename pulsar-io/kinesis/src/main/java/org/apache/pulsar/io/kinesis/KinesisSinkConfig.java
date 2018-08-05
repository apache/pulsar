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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class KinesisSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String awsEndpoint;
    private String awsRegion;
    private String awsKinesisStreamName;
    private String awsCredentialPluginName;
    private String awsCredentialPluginParam;
    private MessageFormat messageFormat = MessageFormat.ONLY_RAW_PAYLOAD; // default : ONLY_RAW_PAYLOAD
    private boolean retainOrdering;

    public static KinesisSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), KinesisSinkConfig.class);
    }

    public static KinesisSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), KinesisSinkConfig.class);
    }
    
    /**
     * Message format in which kinesis-sink converts pulsar-message and publishes to kinesis stream.
     *
     */
    public static enum MessageFormat {
        /**
         * Kinesis sink directly publishes pulsar-payload as a message into the kinesis-stream
         */
        ONLY_RAW_PAYLOAD,
        /**
         * Kinesis sink creates a json payload with message-payload, properties and encryptionCtx and publishes json
         * payload to kinesis stream.
         * 
         * schema: 
         * {"type":"object","properties":{"encryptionCtx":{"type":"object","properties":{"metadata":{"type":"object","additionalProperties":{"type":"string"}},"uncompressedMessageSize":{"type":"integer"},"keysMetadataMap":{"type":"object","additionalProperties":{"type":"object","additionalProperties":{"type":"string"}}},"keysMapBase64":{"type":"object","additionalProperties":{"type":"string"}},"encParamBase64":{"type":"string"},"compressionType":{"type":"string","enum":["NONE","LZ4","ZLIB"]},"batchSize":{"type":"integer"},"algorithm":{"type":"string"}}},"payloadBase64":{"type":"string"},"properties":{"type":"object","additionalProperties":{"type":"string"}}}}
         * Example:
         * {"payloadBase64":"cGF5bG9hZA==","properties":{"prop1":"value"},"encryptionCtx":{"keysMapBase64":{"key1":"dGVzdDE=","key2":"dGVzdDI="},"keysMetadataMap":{"key1":{"ckms":"cmks-1","version":"v1"},"key2":{"ckms":"cmks-2","version":"v2"}},"metadata":{"ckms":"cmks-1","version":"v1"},"encParamBase64":"cGFyYW0=","algorithm":"algo","compressionType":"LZ4","uncompressedMessageSize":10,"batchSize":10}}
         * 
         * 
         */
        FULL_MESSAGE_IN_JSON,
        /**
         * Kinesis sink sends message serialized in flat-buffer.
         */
        FULL_MESSAGE_IN_FB;
    }
}