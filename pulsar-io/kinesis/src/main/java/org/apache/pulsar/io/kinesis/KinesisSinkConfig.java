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
package org.apache.pulsar.io.kinesis;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import software.amazon.kinesis.producer.KinesisProducerConfiguration;

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
        required = false,
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

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Path to the native Amazon Kinesis Producer Library (KPL) binary.\n"
                    + "Only use this setting if you want to use a custom build of the native code.\n"
                    + "This setting can also be set with the environment variable `PULSAR_IO_KINESIS_KPL_1_0_PATH`"
                    + "or `PULSAR_IO_KINESIS_KPL_PATH`.\n"
                    + "If not set, the Kinesis sink will use the built-in native executable."
    )
    private String nativeExecutable = resolveDefaultKinesisProducerLibraryPath();

    private static String resolveDefaultKinesisProducerLibraryPath() {
        // Prefer PULSAR_IO_KINESIS_KPL_1_0_PATH environment variable over PULSAR_IO_KINESIS_KPL_PATH.
        // This setting supports building a Pulsar Functions base image that is used to run different Pulsar IO Kinesis
        // sink versions. The older versions of Pulsar IO Kinesis sink can continue to use the binary configured with
        // PULSAR_IO_KINESIS_KPL_PATH, pointing to a 0.15.12 native executable. The newer versions of Pulsar IO Kinesis
        // sink can use the binary configured with PULSAR_IO_KINESIS_KPL_1_0_PATH, pointing to a 1.0.4
        // native executable.
        String kplPath = System.getenv("PULSAR_IO_KINESIS_KPL_1_0_PATH");
        if (isNotBlank(kplPath)) {
            return kplPath;
        }
        return System.getenv("PULSAR_IO_KINESIS_KPL_PATH");
    }

    public static KinesisSinkConfig load(Map<String, Object> config, SinkContext sinkContext) {
        KinesisSinkConfig kinesisSinkConfig = IOConfigUtils.loadWithSecrets(config, KinesisSinkConfig.class, sinkContext);
        checkArgument(isNotBlank(kinesisSinkConfig.getAwsRegion())
                        || (isNotBlank(kinesisSinkConfig.getAwsEndpoint()) && isNotBlank(kinesisSinkConfig.getCloudwatchEndpoint())),
                "Either \"awsRegion\" must be set OR all of [\"awsEndpoint\", \"cloudwatchEndpoint\"] must be set.");
        return kinesisSinkConfig;
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

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Custom AWS STS endpoint"
    )
    private String awsStsEndpoint = "";

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Custom AWS STS port to connect to"
    )
    private Integer awsStsPort;

    @FieldDoc(
            defaultValue = "true",
            help = "Enable aggregation. With aggregation, multiple user records could be packed into a single\n"
                    + " KinesisRecord. If disabled, each user record is sent in its own KinesisRecord.")
    private boolean aggregationEnabled = true;

    @FieldDoc(
            defaultValue = "500",
            help = "Maximum number of items to pack into an PutRecords request.")
    private long collectionMaxCount = 500L;

    @FieldDoc(
            defaultValue = "5242880",
            help = "Maximum amount of data to send with a PutRecords request. Records larger than the limit will\n"
                    + "still be sent, but will not be grouped with others.")
    private long collectionMaxSize = 5242880L;

    @FieldDoc(
            defaultValue = "6000",
            help = "Timeout (milliseconds) for establishing TLS connections.")
    private long connectTimeout = 6000L;

    @FieldDoc(
            defaultValue = "5000",
            help = "How often to refresh credentials (in milliseconds). During a refresh, credentials are retrieved\n"
                    + "from any SDK credentials providers attached to the wrapper and pushed to the core.")
    private long credentialsRefreshDelay = 5000L;

    @FieldDoc(
            defaultValue = "24",
            help = "Maximum number of connections to open to the backend. HTTP requests are sent in parallel\n"
                    + "over multiple connections.")
    private long maxConnections = 24L;

    @FieldDoc(
            defaultValue = "1",
            help = "Minimum number of connections to keep open to the backend.")
    private long minConnections = 1L;

    @FieldDoc(
            defaultValue = "150",
            help = """
                    Limits the maximum allowed put rate for a shard, as a percentage of the backend limits. The
                    default value of 150% is chosen to allow a single producer instance to completely saturate the
                    allowance for a shard. This is an aggressive setting. If you prefer to reduce throttling
                    errors rather than completely saturate the shard, consider reducing this setting.""")
    private long rateLimit = 150L;

    @FieldDoc(
            defaultValue = "30000",
            help = """
                    Set a time-to-live on records (milliseconds). Records that do not get successfully put within the
                    limit are failed and retried by KinesisSink. This should be set lower than the Pulsar source's
                    timeoutMs to minimize the risk of duplicate records and to control heap memory usage in the Kinesis
                    sink, especially during re-deliveries.""")
    private long recordTtl = 30000L;

    @FieldDoc(
            defaultValue = "6000",
            help = """
                    The maximum total time (milliseconds) elapsed between when we begin a HTTP request and receiving
                    all of the response. If it goes over, the request will be timed-out. Note that a timed-out
                    request may actually succeed at the backend. Retrying then leads to duplicates. Setting the
                    timeout too low will therefore increase the probability of duplicates.""")
    private long requestTimeout = 6000L;

    @FieldDoc(
            defaultValue = "",
            help = "Extra KinesisProducerConfiguration parameters. See https://javadoc.io/static/software.amazon.kinesis/amazon-kinesis-producer/1.0.4/software/amazon/kinesis/producer/KinesisProducerConfiguration.html for all the available parameters."
                    + "Parameters that are explicitly set take preference over extra config.")
    private Map<String, String> extraKinesisProducerConfig = new HashMap<>();

    public static KinesisProducerConfiguration loadExtraKinesisProducerConfig(Map<String, String> map)
            throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsString(map), KinesisProducerConfiguration.class);
    }
}
