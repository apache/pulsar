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
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.PulsarClientException.InvalidConfigurationException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

import com.google.common.collect.Lists;
import lombok.NonNull;

public class ConsumerBuilderImpl<T> implements ConsumerBuilder<T> {

    private final PulsarClientImpl client;
    private ConsumerConfigurationData<T> conf;
    private final Schema<T> schema;
    private List<ConsumerInterceptor<T>> interceptorList;

    private static long MIN_ACK_TIMEOUT_MILLIS = 1000;
    private static long DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER = 30000L;


    public ConsumerBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
        this(client, new ConsumerConfigurationData<T>(), schema);
    }

    ConsumerBuilderImpl(PulsarClientImpl client, ConsumerConfigurationData<T> conf, Schema<T> schema) {
        this.client = client;
        this.conf = conf;
        this.schema = schema;
    }

    @Override
    public ConsumerBuilder<T> loadConf(Map<String, Object> config) {
        this.conf = ConfigurationDataUtils.loadData(config, conf, ConsumerConfigurationData.class);
        return this;
    }

    @Override
    public ConsumerBuilder<T> clone() {
        return new ConsumerBuilderImpl<>(client, conf.clone(), schema);
    }

    @Override
    public Consumer<T> subscribe() throws PulsarClientException {
        try {
            return subscribeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Consumer<T>> subscribeAsync() {
        if (conf.getTopicNames().isEmpty() && conf.getTopicsPattern() == null) {
            return FutureUtil
                    .failedFuture(new InvalidConfigurationException("Topic name must be set on the consumer builder"));
        }

        if (StringUtils.isBlank(conf.getSubscriptionName())) {
            return FutureUtil.failedFuture(
                    new InvalidConfigurationException("Subscription name must be set on the consumer builder"));
        }
        return interceptorList == null || interceptorList.size() == 0 ?
                client.subscribeAsync(conf, schema, null) :
                client.subscribeAsync(conf, schema, new ConsumerInterceptors<>(interceptorList));
    }

    @Override
    public ConsumerBuilder<T> topic(String... topicNames) {
        checkArgument(topicNames != null && topicNames.length > 0,
                "Passed in topicNames should not be null or empty.");
        Arrays.stream(topicNames).forEach(topicName ->
                checkArgument(StringUtils.isNotBlank(topicName), "topicNames cannot have blank topic"));
        conf.getTopicNames().addAll(Lists.newArrayList(topicNames));
        return this;
    }

    @Override
    public ConsumerBuilder<T> topics(List<String> topicNames) {
        checkArgument(topicNames != null && !topicNames.isEmpty(),
                "Passed in topicNames list should not be null or empty.");
        topicNames.stream().forEach(topicName ->
                checkArgument(StringUtils.isNotBlank(topicName), "topicNames cannot have blank topic"));
        conf.getTopicNames().addAll(topicNames);
        return this;
    }

    @Override
    public ConsumerBuilder<T> topicsPattern(Pattern topicsPattern) {
        checkArgument(conf.getTopicsPattern() == null, "Pattern has already been set.");
        conf.setTopicsPattern(topicsPattern);
        return this;
    }

    @Override
    public ConsumerBuilder<T> topicsPattern(String topicsPattern) {
        checkArgument(conf.getTopicsPattern() == null, "Pattern has already been set.");
        conf.setTopicsPattern(Pattern.compile(topicsPattern));
        return this;
    }

    @Override
    public ConsumerBuilder<T> subscriptionName(String subscriptionName) {
        checkArgument(StringUtils.isNotBlank(subscriptionName), "subscriptionName cannot be blank");
        conf.setSubscriptionName(subscriptionName);
        return this;
    }

    @Override
    public ConsumerBuilder<T> ackTimeout(long ackTimeout, TimeUnit timeUnit) {
        checkArgument(ackTimeout == 0 || timeUnit.toMillis(ackTimeout) >= MIN_ACK_TIMEOUT_MILLIS,
                "Ack timeout should be greater than " + MIN_ACK_TIMEOUT_MILLIS + " ms");
        conf.setAckTimeoutMillis(timeUnit.toMillis(ackTimeout));
        return this;
    }

    @Override
    public ConsumerBuilder<T> negativeAckRedeliveryDelay(long redeliveryDelay, TimeUnit timeUnit) {
        checkArgument(redeliveryDelay >= 0, "redeliveryDelay needs to be >= 0");
        conf.setNegativeAckRedeliveryDelayMicros(timeUnit.toMicros(redeliveryDelay));
        return this;
    }

    @Override
    public ConsumerBuilder<T> subscriptionType(@NonNull SubscriptionType subscriptionType) {
        conf.setSubscriptionType(subscriptionType);
        return this;
    }

    @Override
    public ConsumerBuilder<T> messageListener(@NonNull MessageListener<T> messageListener) {
        conf.setMessageListener(messageListener);
        return this;
    }

    @Override
    public ConsumerBuilder<T> consumerEventListener(@NonNull ConsumerEventListener consumerEventListener) {
        conf.setConsumerEventListener(consumerEventListener);
        return this;
    }

    @Override
    public ConsumerBuilder<T> cryptoKeyReader(@NonNull CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ConsumerBuilder<T> cryptoFailureAction(@NonNull ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public ConsumerBuilder<T> receiverQueueSize(int receiverQueueSize) {
        checkArgument(receiverQueueSize >= 0, "receiverQueueSize needs to be >= 0");
        conf.setReceiverQueueSize(receiverQueueSize);
        return this;
    }

    @Override
    public ConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit) {
        checkArgument(delay >= 0, "acknowledgmentGroupTime needs to be >= 0");
        conf.setAcknowledgementsGroupTimeMicros(unit.toMicros(delay));
        return this;
    }

    @Override
    public ConsumerBuilder<T> consumerName(String consumerName) {
        checkArgument(StringUtils.isNotBlank(consumerName), "consumerName cannot be blank");
        conf.setConsumerName(consumerName);
        return this;
    }

    @Override
    public ConsumerBuilder<T> priorityLevel(int priorityLevel) {
        checkArgument(priorityLevel >= 0, "priorityLevel needs to be >= 0");
        conf.setPriorityLevel(priorityLevel);
        return this;
    }

    @Override
    public ConsumerBuilder<T> property(String key, String value) {
        checkArgument(StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value),
                "property key/value cannot be blank");
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public ConsumerBuilder<T> properties(@NonNull Map<String, String> properties) {
        checkArgument(!properties.isEmpty(), "properties cannot be empty");
        properties.entrySet().forEach(entry ->
                checkArgument(
                        StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue()),
                        "properties' key/value cannot be blank"));
        conf.getProperties().putAll(properties);
        return this;
    }

    @Override
    public ConsumerBuilder<T> maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions) {
        checkArgument(maxTotalReceiverQueueSizeAcrossPartitions >= 0, "maxTotalReceiverQueueSizeAcrossPartitions needs to be >= 0");
        conf.setMaxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions);
        return this;
    }

    @Override
    public ConsumerBuilder<T> readCompacted(boolean readCompacted) {
        conf.setReadCompacted(readCompacted);
        return this;
    }

    @Override
    public ConsumerBuilder<T> patternAutoDiscoveryPeriod(int periodInMinutes) {
        checkArgument(periodInMinutes >= 0, "periodInMinutes needs to be >= 0");
        conf.setPatternAutoDiscoveryPeriod(periodInMinutes);
        return this;
    }

	@Override
	public ConsumerBuilder<T> subscriptionInitialPosition(@NonNull SubscriptionInitialPosition
                                                                      subscriptionInitialPosition) {
        conf.setSubscriptionInitialPosition(subscriptionInitialPosition);
		return this;
	}

    @Override
    public ConsumerBuilder<T> subscriptionTopicsMode(@NonNull RegexSubscriptionMode mode) {
        conf.setRegexSubscriptionMode(mode);
        return this;
    }

    @Override
    public ConsumerBuilder<T> replicateSubscriptionState(boolean replicateSubscriptionState) {
        conf.setReplicateSubscriptionState(replicateSubscriptionState);
        return this;
    }

    @Override
    public ConsumerBuilder<T> intercept(ConsumerInterceptor<T>... interceptors) {
        if (interceptorList == null) {
            interceptorList = new ArrayList<>();
        }
        interceptorList.addAll(Arrays.asList(interceptors));
        return this;
    }

    @Override
    public ConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy) {
        if (deadLetterPolicy != null) {
            if (conf.getAckTimeoutMillis() == 0) {
                conf.setAckTimeoutMillis(DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER);
            }
            conf.setDeadLetterPolicy(deadLetterPolicy);
        }
        return this;
    }

    @Override
    public ConsumerBuilder<T> autoUpdatePartitions(boolean autoUpdate) {
        conf.setAutoUpdatePartitions(autoUpdate);
        return this;
    }

    public ConsumerConfigurationData<T> getConf() {
        return conf;
    }
}
