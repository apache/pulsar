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
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@EqualsAndHashCode(callSuper = true)
public class KinesisSinkConfig extends BaseKinesisConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Kinesis end-point port. It can be found at https://docs.aws.amazon.com/general/latest/gr/rande.html"
    )
    private Integer awsEndpointPort;

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "Tell to Kinesis Client to skip certificate validation. This is useful while performing local tests, it's recommended to always validate certificates in production environments."
    )
    private Boolean skipCertificateValidation = false;

    @FieldDoc(
        required = true,
        defaultValue = "ONLY_RAW_PAYLOAD",
        help = "Message format in which kinesis sink converts pulsar messages and publishes to kinesis streams.\n"
            + "  #\n"
            + "  # The available messages formats are: \n"
            + "  #\n"
            + "  # - ONLY_RAW_PAYLOAD \n"
            + "  #\n"
            + "  #   Kinesis sink directly publishes pulsar message payload as a message into the configured kinesis stream. \n"
            + "  #\n"
            + "  # - FULL_MESSAGE_IN_JSON \n"
            + "  #\n"
            + "  #   Kinesis sink creates a json payload with pulsar message payload, properties and encryptionCtx, \n"
            + "  #   and publishes json payload into the configured kinesis stream.\n"
            + "  #\n"
            + "  # - FULL_MESSAGE_IN_FB \n"
            + "  #\n"
            + "  #   Kinesis sink creates a flatbuffer serialized paylaod with pulsar message payload, \n"
            + "  #   properties and encryptionCtx, and publishes flatbuffer payload into the configured kinesis stream."
            + "  #\n"
            + "  # - FULL_MESSAGE_IN_JSON_EXPAND_VALUE \n"
            + "  #\n"
            + "  #   Kinesis sink sends a JSON structure containing the record topic name, key, payload, properties and event time.\n"
            + "  #   The record schema is used to convert the value to JSON."
    )
    private MessageFormat messageFormat = MessageFormat.ONLY_RAW_PAYLOAD; // default : ONLY_RAW_PAYLOAD

    @FieldDoc(
            defaultValue = "true",
            help = "Value that indicates that only properties with non-null values are to be included when using "
                + "MessageFormat.FULL_MESSAGE_IN_JSON_EXPAND_VALUE."
    )
    private boolean jsonIncludeNonNulls = true;

    @FieldDoc(
            defaultValue = "false",
            help = "When set to true and the message format is FULL_MESSAGE_IN_JSON_EXPAND_VALUE the output JSON will be flattened."
    )
    private boolean jsonFlatten = false;

    @FieldDoc(
        defaultValue = "false",
        help = "A flag to tell Pulsar IO to retain ordering when moving messages from Pulsar to Kinesis")
    private boolean retainOrdering = false;

    @FieldDoc(
            defaultValue = "100",
            help = "The initial delay(in milliseconds) between retries.")
    private long retryInitialDelayInMillis = 100;

    @FieldDoc(
            defaultValue = "60000",
            help = "The maximum delay(in milliseconds) between retries.")
    private long retryMaxDelayInMillis = 60000;

    public static KinesisSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), KinesisSinkConfig.class);
    }

    public enum MessageFormat {
        /**
         * Kinesis sink directly publishes pulsar-payload as a message into the kinesis-stream.
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
        FULL_MESSAGE_IN_FB,
        /**
         * Kinesis sink sends a JSON structure containing the record topic name, key, payload, properties and event time.
         * The record schema is used to convert the value to JSON.
         *
         * Example for primitive schema:
         * {"topicName":"my-topic","key":"message-key","payload":"message-value","properties":{"prop-key":"prop-value"},"eventTime":1648502845803}
         *
         * Example for AVRO or JSON schema:
         * {"topicName":"my-topic","key":"message-key","payload":{"c":"1","d":1,"e":{"a":"a"}},"properties":{"prop-key":"prop-value"},"eventTime":1648502845803}
         *
         * Example for KeyValue schema:
         * {"topicName":"my-topic","key":"message-key","payload":{"value":{"c":"1","d":1,"e":{"a":"a"}},"key":{"a":"1","b":1}},"properties":{"prop-key":"prop-value"},"eventTime":1648502845803}
         */
        FULL_MESSAGE_IN_JSON_EXPAND_VALUE
    }

}
