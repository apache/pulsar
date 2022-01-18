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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.NegativeAckRedeliveryBackoff;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidConfigurationException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

@Getter(AccessLevel.PUBLIC)
public class ConsumerBuilderImpl<T> implements ConsumerBuilder<T> {

    private final PulsarClientImpl client;
    private ConsumerConfigurationData<T> conf;
    private final Schema<T> schema;
    private List<ConsumerInterceptor<T>> interceptorList;

    private static long MIN_ACK_TIMEOUT_MILLIS = 1000;
    private static long MIN_TICK_TIME_MILLIS = 100;
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

        if (conf.getKeySharedPolicy() != null && conf.getSubscriptionType() != SubscriptionType.Key_Shared) {
            return FutureUtil.failedFuture(
                    new InvalidConfigurationException("KeySharedPolicy must set with KeyShared subscription"));
        }
        if(conf.isRetryEnable() && conf.getTopicNames().size() > 0 ) {
            TopicName topicFirst = TopicName.get(conf.getTopicNames().iterator().next());
            String retryLetterTopic = topicFirst + "-" + conf.getSubscriptionName() + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX;
            String deadLetterTopic = topicFirst + "-" + conf.getSubscriptionName() + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX;

            //Issue 9327: do compatibility check in case of the default retry and dead letter topic name changed
            String oldRetryLetterTopic = topicFirst.getNamespace() + "/" + conf.getSubscriptionName() + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX;
            String oldDeadLetterTopic = topicFirst.getNamespace() + "/" + conf.getSubscriptionName() + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX;
            try {
                if (client.getPartitionedTopicMetadata(oldRetryLetterTopic)
                        .get(client.conf.getOperationTimeoutMs(), TimeUnit.MILLISECONDS).partitions > 0) {
                    retryLetterTopic = oldRetryLetterTopic;
                }
                if (client.getPartitionedTopicMetadata(oldDeadLetterTopic)
                        .get(client.conf.getOperationTimeoutMs(), TimeUnit.MILLISECONDS).partitions > 0) {
                    deadLetterTopic = oldDeadLetterTopic;
                }
            } catch (InterruptedException | TimeoutException e) {
                return FutureUtil.failedFuture(e);
            } catch (ExecutionException e) {
                return FutureUtil.failedFuture(e.getCause());
            }

            if(conf.getDeadLetterPolicy() == null) {
                conf.setDeadLetterPolicy(DeadLetterPolicy.builder()
                                        .maxRedeliverCount(RetryMessageUtil.MAX_RECONSUMETIMES)
                                        .retryLetterTopic(retryLetterTopic)
                                        .deadLetterTopic(deadLetterTopic)
                                        .build());
            } else {
                if (StringUtils.isBlank(conf.getDeadLetterPolicy().getRetryLetterTopic())) {
                    conf.getDeadLetterPolicy().setRetryLetterTopic(retryLetterTopic);
                }
                if (StringUtils.isBlank(conf.getDeadLetterPolicy().getDeadLetterTopic())) {
                    conf.getDeadLetterPolicy().setDeadLetterTopic(deadLetterTopic);
                }
            }
            conf.getTopicNames().add(conf.getDeadLetterPolicy().getRetryLetterTopic());
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
        conf.getTopicNames().addAll(Arrays.stream(topicNames).map(StringUtils::trim)
                .collect(Collectors.toList()));
        return this;
    }

    @Override
    public ConsumerBuilder<T> topics(List<String> topicNames) {
        checkArgument(topicNames != null && !topicNames.isEmpty(),
                "Passed in topicNames list should not be null or empty.");
        topicNames.stream().forEach(topicName ->
                checkArgument(StringUtils.isNotBlank(topicName), "topicNames cannot have blank topic"));
        conf.getTopicNames().addAll(topicNames.stream().map(StringUtils::trim).collect(Collectors.toList()));
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
    public ConsumerBuilder<T> subscriptionProperties(Map<String, String> subscriptionProperties) {
        checkArgument(subscriptionProperties != null, "subscriptionProperties cannot be null");
        conf.setSubscriptionProperties(Collections.unmodifiableMap(subscriptionProperties));
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
    public ConsumerBuilder<T> isAckReceiptEnabled(boolean isAckReceiptEnabled) {
        conf.setAckReceiptEnabled(isAckReceiptEnabled);
        return this;
    }

    @Override
    public ConsumerBuilder<T> ackTimeoutTickTime(long tickTime, TimeUnit timeUnit) {
        checkArgument(timeUnit.toMillis(tickTime) >= MIN_TICK_TIME_MILLIS,
                "Ack timeout tick time should be greater than " + MIN_TICK_TIME_MILLIS + " ms");
        conf.setTickDurationMillis(timeUnit.toMillis(tickTime));
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
    public ConsumerBuilder<T> subscriptionMode(@NonNull SubscriptionMode subscriptionMode) {
        conf.setSubscriptionMode(subscriptionMode);
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
    public ConsumerBuilder<T> defaultCryptoKeyReader(String privateKey) {
        checkArgument(StringUtils.isNotBlank(privateKey), "privateKey cannot be blank");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().defaultPrivateKey(privateKey).build());
    }

    @Override
    public ConsumerBuilder<T> defaultCryptoKeyReader(@NonNull Map<String, String> privateKeys) {
        checkArgument(!privateKeys.isEmpty(), "privateKeys cannot be empty");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().privateKeys(privateKeys).build());
    }

    @Override
    public ConsumerBuilder<T> messageCrypto(@NonNull MessageCrypto messageCrypto) {
        conf.setMessageCrypto(messageCrypto);
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
    public ConsumerBuilder<T> maxPendingChuckedMessage(int maxPendingChuckedMessage) {
        conf.setMaxPendingChunkedMessage(maxPendingChuckedMessage);
        return this;
    }

    @Override
    public ConsumerBuilder<T> maxPendingChunkedMessage(int maxPendingChunkedMessage) {
        conf.setMaxPendingChunkedMessage(maxPendingChunkedMessage);
        return this;
    }

    @Override
    public ConsumerBuilder<T> autoAckOldestChunkedMessageOnQueueFull(boolean autoAckOldestChunkedMessageOnQueueFull) {
        conf.setAutoAckOldestChunkedMessageOnQueueFull(autoAckOldestChunkedMessageOnQueueFull);
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
        patternAutoDiscoveryPeriod(periodInMinutes, TimeUnit.MINUTES);
        return this;
    }

    @Override
    public ConsumerBuilder<T> patternAutoDiscoveryPeriod(int interval, TimeUnit unit) {
        checkArgument(interval >= 0, "interval needs to be >= 0");
        int intervalSeconds = (int) unit.toSeconds(interval);
        conf.setPatternAutoDiscoveryPeriod(intervalSeconds);
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

    @Override
    public ConsumerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit) {
        conf.setAutoUpdatePartitionsIntervalSeconds(interval, unit);
        return this;
    }

    @Override

    public ConsumerBuilder<T> startMessageIdInclusive() {
        conf.setResetIncludeHead(true);
        return this;
    }

    public ConsumerBuilder<T> batchReceivePolicy(BatchReceivePolicy batchReceivePolicy) {
        checkArgument(batchReceivePolicy != null, "batchReceivePolicy must not be null.");
        batchReceivePolicy.verify();
        conf.setBatchReceivePolicy(batchReceivePolicy);
        return this;
    }

    @Override
    public String toString() {
        return conf != null ? conf.toString() : "";
    }

    @Override
    public ConsumerBuilder<T> keySharedPolicy(KeySharedPolicy keySharedPolicy) {
        keySharedPolicy.validate();
        conf.setKeySharedPolicy(keySharedPolicy);
        return this;
    }

    @Override
    public ConsumerBuilder<T> enableRetry(boolean retryEnable) {
        conf.setRetryEnable(retryEnable);
        return this;
    }

    @Override
    public ConsumerBuilder<T> enableBatchIndexAcknowledgment(boolean batchIndexAcknowledgmentEnabled) {
        conf.setBatchIndexAckEnabled(batchIndexAcknowledgmentEnabled);
        return this;
    }

    @Override
    public ConsumerBuilder<T> expireTimeOfIncompleteChunkedMessage(long duration, TimeUnit unit) {
        conf.setExpireTimeOfIncompleteChunkedMessageMillis(unit.toMillis(duration));
        return this;
    }

    @Override
    public ConsumerBuilder<T> poolMessages(boolean poolMessages) {
        conf.setPoolMessages(poolMessages);
        return this;
    }

    @Override
    public ConsumerBuilder<T> messagePayloadProcessor(MessagePayloadProcessor payloadProcessor) {
        conf.setPayloadProcessor(payloadProcessor);
        return this;
    }

    @Override
    public ConsumerBuilder<T> negativeAckRedeliveryBackoff(NegativeAckRedeliveryBackoff negativeAckRedeliveryBackoff) {
        checkArgument(negativeAckRedeliveryBackoff != null, "negativeAckRedeliveryBackoff must not be null.");
        conf.setNegativeAckRedeliveryBackoff(negativeAckRedeliveryBackoff);
        return this;
    }

    @Override
    public ConsumerBuilder<T> startPaused(boolean paused) {
        conf.setStartPaused(paused);
        return this;
    }
}
