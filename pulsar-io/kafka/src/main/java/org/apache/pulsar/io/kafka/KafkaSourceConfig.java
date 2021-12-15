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

package org.apache.pulsar.io.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class KafkaSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help =
            "A comma-separated list of host and port pairs that are the addresses of "
          + "the Kafka brokers that a Kafka client connects to initially bootstrap itself")
    private String bootstrapServers;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Protocol used to communicate with Kafka brokers.")
    private String securityProtocol;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "SASL mechanism used for Kafka client connections.")
    private String saslMechanism;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "JAAS login context parameters for SASL connections in the format used by JAAS configuration files.")
    private String saslJaasConfig;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The list of protocols enabled for SSL connections.")
    private String sslEnabledProtocols;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The endpoint identification algorithm to validate server hostname using server certificate.")
    private String sslEndpointIdentificationAlgorithm;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The location of the trust store file.")
    private String sslTruststoreLocation;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The password for the trust store file.")
    private String sslTruststorePassword;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help =
            "A string that uniquely identifies the group of consumer processes to which this consumer belongs.")
    private String groupId;
    @FieldDoc(
        defaultValue = "1",
        help =
            "The minimum amount of data the server should return for a fetch request.")
    private long fetchMinBytes = 1L;
    @FieldDoc(
        defaultValue = "5000",
        help =
            "The frequency in milliseconds that the consumer offsets are auto-committed to Kafka "
          + "if autoCommitEnabled is set to true.")
    private long autoCommitIntervalMs = 5000L;
    @FieldDoc(
        defaultValue = "30000",
        help =
            "The timeout used to detect failures when using Kafka's group management facilities.")
    private long sessionTimeoutMs = 30000L;
    @FieldDoc(
        defaultValue = "3000",
        help =
            "The interval between heartbeats to the consumer when using Kafka's group management facilities. "
                + "The value must be lower than session timeout.")
    private long heartbeatIntervalMs = 3000L;
    @FieldDoc(
        defaultValue = "true",
        help =
            "If true the consumer's offset will be periodically committed in the background.")
    private boolean autoCommitEnabled = true;
    @FieldDoc(
        required = true,
        defaultValue = "",
        help =
            "The Kafka topic that is used for Pulsar moving messages to.")
    private String topic;
    @FieldDoc(
        defaultValue = "org.apache.kafka.common.serialization.StringDeserializer",
        help =
            "The deserializer class for Kafka consumer to deserialize keys.")
    private String keyDeserializationClass = "org.apache.kafka.common.serialization.StringDeserializer";
    @FieldDoc(
        defaultValue = "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        help =
            "The deserializer class for Kafka consumer to deserialize values. You typically shouldn't care this. "
                + "Since the deserializer will be set by a specific implementation of `KafkaAbstractSource`.")
    private String valueDeserializationClass = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    @FieldDoc(
            defaultValue = "earliest",
            help =
                    "The default offset reset policy.")
    private String autoOffsetReset = "earliest";
    @FieldDoc(
        defaultValue = "",
        help =
            "The consumer config properties to be passed to Consumer. Note that other properties specified "
                + "in the connector config file take precedence over this config.")
    private Map<String, Object> consumerConfigProperties;

    public static KafkaSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), KafkaSourceConfig.class);
    }

    public static KafkaSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), KafkaSourceConfig.class);
    }
}