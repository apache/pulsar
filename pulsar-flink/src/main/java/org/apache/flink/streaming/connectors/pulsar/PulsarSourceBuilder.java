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
package org.apache.flink.streaming.connectors.pulsar;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

/**
 * A class for building a pulsar source.
 */
@PublicEvolving
public class PulsarSourceBuilder<T> {

    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final long ACKNOWLEDGEMENT_BATCH_SIZE = 100;
    private static final long MAX_ACKNOWLEDGEMENT_BATCH_SIZE = 1000;
    private static final String SUBSCRIPTION_NAME = "flink-sub";

    final DeserializationSchema<T> deserializationSchema;

    ClientConfigurationData clientConfigurationData;
    ConsumerConfigurationData<byte[]> consumerConfigurationData;

    long acknowledgementBatchSize = ACKNOWLEDGEMENT_BATCH_SIZE;

    private PulsarSourceBuilder(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;

        clientConfigurationData = new ClientConfigurationData();
        consumerConfigurationData = new ConsumerConfigurationData<>();
        clientConfigurationData.setServiceUrl(SERVICE_URL);
        consumerConfigurationData.setTopicNames(new TreeSet<>());
        consumerConfigurationData.setSubscriptionName(SUBSCRIPTION_NAME);
        consumerConfigurationData.setSubscriptionInitialPosition(SubscriptionInitialPosition.Latest);
    }

    /**
     * Sets the pulsar service url to connect to. Defaults to pulsar://localhost:6650.
     *
     * @param serviceUrl service url to connect to
     * @return this builder
     */
    public PulsarSourceBuilder<T> serviceUrl(String serviceUrl) {
        Preconditions.checkArgument(StringUtils.isNotBlank(serviceUrl), "serviceUrl cannot be blank");
        this.clientConfigurationData.setServiceUrl(serviceUrl);
        return this;
    }

    /**
     * Sets topics to consumer from. This is required.
     *
     * <p>Topic names (https://pulsar.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
     * are in the following format:
     * {persistent|non-persistent}://tenant/namespace/topic
     *
     * @param topics the topic to consumer from
     * @return this builder
     */
    public PulsarSourceBuilder<T> topic(String... topics) {
        Preconditions.checkArgument(topics != null && topics.length > 0,
                "topics cannot be blank");
        for (String topic : topics) {
            Preconditions.checkArgument(StringUtils.isNotBlank(topic), "topicNames cannot have blank topic");
        }
        this.consumerConfigurationData.getTopicNames().addAll(Arrays.asList(topics));
        return this;
    }

    /**
     * Sets topics to consumer from. This is required.
     *
     * <p>Topic names (https://pulsar.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
     * are in the following format:
     * {persistent|non-persistent}://tenant/namespace/topic
     *
     * @param topics the topic to consumer from
     * @return this builder
     */
    public PulsarSourceBuilder<T> topics(List<String> topics) {
        Preconditions.checkArgument(topics != null && !topics.isEmpty(), "topics cannot be blank");
        topics.forEach(topicName ->
                Preconditions.checkArgument(StringUtils.isNotBlank(topicName), "topicNames cannot have blank topic"));
        this.consumerConfigurationData.getTopicNames().addAll(topics);
        return this;
    }

    /**
     * Use topic pattern to config sets of topics to consumer.
     *
     * <p>Topic names (https://pulsar.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
     * are in the following format:
     * {persistent|non-persistent}://tenant/namespace/topic
     *
     * @param topicsPattern topic pattern to consumer from
     * @return this builder
     */
    public PulsarSourceBuilder<T> topicsPattern(Pattern topicsPattern) {
        Preconditions.checkArgument(topicsPattern != null, "Param topicsPattern cannot be null");
        Preconditions.checkArgument(this.consumerConfigurationData.getTopicsPattern() == null,
            "Pattern has already been set.");
        this.consumerConfigurationData.setTopicsPattern(topicsPattern);
        return this;
    }

    /**
     * Use topic pattern to config sets of topics to consumer.
     *
     * <p>Topic names (https://pulsar.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
     * are in the following format:
     * {persistent|non-persistent}://tenant/namespace/topic
     *
     * @param topicsPattern topic pattern string to consumer from
     * @return this builder
     */
    public PulsarSourceBuilder<T> topicsPatternString(String topicsPattern) {
        Preconditions.checkArgument(StringUtils.isNotBlank(topicsPattern), "Topics pattern string cannot be blank");
        Preconditions.checkArgument(this.consumerConfigurationData.getTopicsPattern() == null,
            "Pattern has already been set.");
        this.consumerConfigurationData.setTopicsPattern(Pattern.compile(topicsPattern));
        return this;
    }

    /**
     * Sets the subscription name for the topic consumer. Defaults to flink-sub.
     *
     * @param subscriptionName the subscription name for the topic consumer
     * @return this builder
     */
    public PulsarSourceBuilder<T> subscriptionName(String subscriptionName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(subscriptionName),
                "subscriptionName cannot be blank");
        this.consumerConfigurationData.setSubscriptionName(subscriptionName);
        return this;
    }

    /**
     * Sets the subscription initial position for the topic consumer.
     * Default is {@link SubscriptionInitialPosition#Latest}
     *
     * @param initialPosition the subscription initial position.
     * @return this builder
     */
    public PulsarSourceBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition initialPosition) {
        Preconditions.checkNotNull(initialPosition, "subscription initial position cannot be null");
        this.consumerConfigurationData.setSubscriptionInitialPosition(initialPosition);
        return this;
    }

    /**
     * Sets the number of messages to receive before acknowledging. This defaults to 100. This
     * value is only used when checkpointing is disabled.
     *
     * @param size number of messages to receive before acknowledging
     * @return this builder
     */
    public PulsarSourceBuilder<T> acknowledgementBatchSize(long size) {
        if (size > 0 && size <= MAX_ACKNOWLEDGEMENT_BATCH_SIZE) {
            acknowledgementBatchSize = size;
            return this;
        }
        throw new IllegalArgumentException(
            "acknowledgementBatchSize can only take values > 0 and <= " + MAX_ACKNOWLEDGEMENT_BATCH_SIZE);
    }

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     *
     * @param authentication an instance of the {@link Authentication} provider already constructed
     * @return this builder
     */
    public PulsarSourceBuilder<T> authentication(Authentication authentication) {
        Preconditions.checkArgument(authentication != null,
                "authentication instance can not be null, use new AuthenticationDisabled() to disable authentication");
        this.clientConfigurationData.setAuthentication(authentication);
        return this;
    }

    /**
     * Configure the authentication provider to use in the Pulsar client instance.
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @return this builder
     * @throws PulsarClientException.UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    public PulsarSourceBuilder<T> authentication(String authPluginClassName, String authParamsString)
            throws PulsarClientException.UnsupportedAuthenticationException {
        Preconditions.checkArgument(StringUtils.isNotBlank(authPluginClassName),
                "Authentication-Plugin class name can not be blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(authParamsString),
                "Authentication-Plugin parameters can not be blank");
        this.clientConfigurationData
            .setAuthentication(AuthenticationFactory.create(authPluginClassName, authParamsString));
        return this;
    }

    /**
     * Configure the authentication provider to use in the Pulsar client instance
     * using a config map.
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @return this builder
     * @throws PulsarClientException.UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    public PulsarSourceBuilder<T> authentication(String authPluginClassName, Map<String, String> authParams)
            throws PulsarClientException.UnsupportedAuthenticationException {
        Preconditions.checkArgument(StringUtils.isNotBlank(authPluginClassName),
                "Authentication-Plugin class name can not be blank");
        Preconditions.checkArgument((authParams != null && !authParams.isEmpty()),
                "parameters to authentication plugin can not be null/empty");
        this.clientConfigurationData.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParams));
        return this;
    }

    /**
     *
     * @param clientConfigurationData All client conf wrapped in a POJO
     * @return this builder
     */
    public PulsarSourceBuilder<T> pulsarAllClientConf(ClientConfigurationData clientConfigurationData) {
        Preconditions.checkNotNull(clientConfigurationData, "client conf should not be null");
        this.clientConfigurationData = clientConfigurationData;
        return this;
    }

    /**
     *
     * @param consumerConfigurationData All consumer conf wrapped in a POJO
     * @return this builder
     */
    public PulsarSourceBuilder<T> pulsarAllConsumerConf(ConsumerConfigurationData consumerConfigurationData) {
        Preconditions.checkNotNull(consumerConfigurationData, "consumer conf should not be null");
        this.consumerConfigurationData = consumerConfigurationData;
        return this;
    }


    public SourceFunction<T> build() throws PulsarClientException{
        Preconditions.checkArgument(StringUtils.isNotBlank(this.clientConfigurationData.getServiceUrl()),
                "a service url is required");
        Preconditions.checkArgument((this.consumerConfigurationData.getTopicNames() != null
                && !this.consumerConfigurationData.getTopicNames().isEmpty())
                || this.consumerConfigurationData.getTopicsPattern() != null,
                "At least one topic or topics pattern is required");
        Preconditions.checkArgument(StringUtils.isNotBlank(this.consumerConfigurationData.getSubscriptionName()),
                "a subscription name is required");

        setTransientFields();

        return new PulsarConsumerSource<>(this);
    }

    private void setTransientFields() throws PulsarClientException {
        setAuth();
    }

    private void setAuth() throws PulsarClientException{
        if (StringUtils.isBlank(this.clientConfigurationData.getAuthPluginClassName())
                || StringUtils.isBlank(this.clientConfigurationData.getAuthParams())) {
            return;
        }

        clientConfigurationData.setAuthentication(
                AuthenticationFactory.create(
                        this.clientConfigurationData.getAuthPluginClassName(),
                        this.clientConfigurationData.getAuthParams()));
    }

    /**
     * Creates a PulsarSourceBuilder.
     *
     * @param deserializationSchema the deserializer used to convert between Pulsar's byte messages and Flink's objects.
     * @return a builder
     */
    public static <T> PulsarSourceBuilder<T> builder(DeserializationSchema<T> deserializationSchema) {
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema cannot be null");
        return new PulsarSourceBuilder<>(deserializationSchema);
    }
}
