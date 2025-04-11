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
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.FetchConsumer;
import org.apache.pulsar.client.api.FetchConsumerBuilder;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class FetchConsumerBuilderImpl<T> implements FetchConsumerBuilder<T>, Cloneable {
    private final PulsarClientImpl client;
    private ConsumerConfigurationData<T> conf;
    private final Schema<T> schema;
    private List<ConsumerInterceptor<T>> interceptorList;
    private volatile boolean interruptedBeforeConsumerCreation;

    public FetchConsumerBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
        this(client, new ConsumerConfigurationData<T>(), schema);
    }

    FetchConsumerBuilderImpl(PulsarClientImpl client, ConsumerConfigurationData<T> conf, Schema<T> schema) {
        checkArgument(schema != null, "Schema should not be null.");
        this.client = client;
        this.conf = conf;
        this.schema = schema;
    }

    @Override
    public FetchConsumerBuilder<T> loadConf(Map<String, Object> config) {
        this.conf = ConfigurationDataUtils.loadData(config, conf, ConsumerConfigurationData.class);
        return this;
    }

    @Override
    public FetchConsumer<T> subscribe() throws PulsarClientException {
        CompletableFuture<FetchConsumer<T>> future = new CompletableFuture<>();
        try {
            subscribeAsync().whenComplete((c, e) -> {
                if (e != null) {
                    // If the subscription fails, there is no need to close the consumer here,
                    // as it will be handled in the subscribeAsync method.
                    future.completeExceptionally(e);
                    return;
                }
                if (interruptedBeforeConsumerCreation) {
                    c.closeAsync().exceptionally(closeEx -> {
                        log.error("Failed to close consumer after interruption", closeEx.getCause());
                        return null;
                    });
                    future.completeExceptionally(new PulsarClientException(
                            "Subscription was interrupted before the consumer could be fully created"));
                } else {
                    future.complete(c);
                }
            });
            return future.get();
        } catch (InterruptedException e) {
            interruptedBeforeConsumerCreation = true;
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    private CompletableFuture<Boolean> checkDlqAlreadyExists(String topic) {
        CompletableFuture<Boolean> existsFuture = new CompletableFuture<>();
        client.getPartitionedTopicMetadata(topic, false, true).thenAccept(metadata -> {
            TopicName topicName = TopicName.get(topic);
            if (topicName.isPersistent()) {
                // Either partitioned or non-partitioned, it exists.
                existsFuture.complete(true);
            } else {
                // If it is a non-persistent topic, return true only it is a partitioned topic.
                existsFuture.complete(metadata != null && metadata.partitions > 0);
            }
        }).exceptionally(ex -> {
            Throwable actEx = FutureUtil.unwrapCompletionException(ex);
            if (actEx instanceof PulsarClientException.NotFoundException
                    || actEx instanceof PulsarClientException.TopicDoesNotExistException
                    || actEx instanceof PulsarAdminException.NotFoundException) {
                existsFuture.complete(false);
            } else {
                existsFuture.completeExceptionally(ex);
            }
            return null;
        });
        return existsFuture;
    }

    @Override
    public CompletableFuture<FetchConsumer<T>> subscribeAsync() {
        if (conf.getTopicNames().isEmpty() && conf.getTopicsPattern() == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                            .InvalidConfigurationException("Topic name must be set on the consumer builder"));
        }

        if (StringUtils.isBlank(conf.getSubscriptionName())) {
            return FutureUtil.failedFuture(new PulsarClientException
                            .InvalidConfigurationException("Subscription name must be set on the consumer builder"));
        }

        if (conf.getKeySharedPolicy() != null && conf.getSubscriptionType() != SubscriptionType.Key_Shared) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .InvalidConfigurationException("KeySharedPolicy must set with KeyShared subscription"));
        }
        if (conf.getBatchReceivePolicy() != null) {
            conf.setReceiverQueueSize(
                    Math.max(conf.getBatchReceivePolicy().getMaxNumMessages(), conf.getReceiverQueueSize()));
        }
        CompletableFuture<Void> applyDLQConfig;
        if (conf.isRetryEnable() && conf.getTopicNames().size() > 0) {
            TopicName topicFirst = TopicName.get(conf.getTopicNames().iterator().next());
            //Issue 9327: do compatibility check in case of the default retry and dead letter topic name changed
            String oldRetryLetterTopic = TopicName.get(topicFirst.getDomain().value(), topicFirst.getNamespaceObject(),
                    conf.getSubscriptionName() + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX).toString();
            String oldDeadLetterTopic = TopicName.get(topicFirst.getDomain().value(), topicFirst.getNamespaceObject(),
                    conf.getSubscriptionName() + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX).toString();
            DeadLetterPolicy deadLetterPolicy = conf.getDeadLetterPolicy();
            if (deadLetterPolicy == null || StringUtils.isBlank(deadLetterPolicy.getRetryLetterTopic())
                    || StringUtils.isBlank(deadLetterPolicy.getDeadLetterTopic())) {
                CompletableFuture<Boolean> retryLetterTopicMetadata = checkDlqAlreadyExists(oldRetryLetterTopic);
                CompletableFuture<Boolean> deadLetterTopicMetadata = checkDlqAlreadyExists(oldDeadLetterTopic);
                applyDLQConfig = CompletableFuture.allOf(retryLetterTopicMetadata, deadLetterTopicMetadata)
                        .thenAccept(__ -> {
                            String retryLetterTopic = RetryMessageUtil.getRetryTopic(topicFirst.toString(),
                                    conf.getSubscriptionName());
                            String deadLetterTopic = RetryMessageUtil.getDLQTopic(topicFirst.toString(),
                                    conf.getSubscriptionName());
                            if (retryLetterTopicMetadata.join()) {
                                retryLetterTopic = oldRetryLetterTopic;
                            }
                            if (deadLetterTopicMetadata.join()) {
                                deadLetterTopic = oldDeadLetterTopic;
                            }
                            if (deadLetterPolicy == null) {
                                conf.setDeadLetterPolicy(DeadLetterPolicy.builder()
                                        .maxRedeliverCount(RetryMessageUtil.MAX_RECONSUMETIMES)
                                        .retryLetterTopic(retryLetterTopic)
                                        .deadLetterTopic(deadLetterTopic)
                                        .build());
                            } else {
                                if (StringUtils.isBlank(deadLetterPolicy.getRetryLetterTopic())) {
                                    conf.getDeadLetterPolicy().setRetryLetterTopic(retryLetterTopic);
                                }
                                if (StringUtils.isBlank(deadLetterPolicy.getDeadLetterTopic())) {
                                    conf.getDeadLetterPolicy().setDeadLetterTopic(deadLetterTopic);
                                }
                            }
                            conf.getTopicNames().add(conf.getDeadLetterPolicy().getRetryLetterTopic());
                        });
            } else {
                conf.getTopicNames().add(conf.getDeadLetterPolicy().getRetryLetterTopic());
                applyDLQConfig = CompletableFuture.completedFuture(null);
            }
        } else {
            applyDLQConfig = CompletableFuture.completedFuture(null);
        }
        return applyDLQConfig.thenCompose(__ -> {
            if (interceptorList == null || interceptorList.size() == 0) {
                return client.fetchConsumerSubscribeAsync(conf, schema, null);
            } else {
                return client.fetchConsumerSubscribeAsync(conf, schema, new ConsumerInterceptors<>(interceptorList));
            }
        });
    }

    @Override
    public FetchConsumerBuilder<T> topic(String... topicNames) {
        checkArgument(topicNames != null && topicNames.length > 0,
                "Passed in topicNames should not be null or empty.");
        return topics(Arrays.stream(topicNames).collect(Collectors.toList()));
    }

    @Override
    public FetchConsumerBuilder<T> topics(List<String> topicNames) {
        checkArgument(topicNames != null && !topicNames.isEmpty(),
                "Passed in topicNames list should not be null or empty.");
        topicNames.stream().forEach(topicName ->
                checkArgument(StringUtils.isNotBlank(topicName), "topicNames cannot have blank topic"));
        conf.getTopicNames().addAll(topicNames.stream().map(StringUtils::trim).collect(Collectors.toList()));
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> subscriptionName(String subscriptionName) {
        checkArgument(StringUtils.isNotBlank(subscriptionName), "subscriptionName cannot be blank");
        conf.setSubscriptionName(subscriptionName);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> subscriptionProperties(Map<String, String> subscriptionProperties) {
        checkArgument(subscriptionProperties != null, "subscriptionProperties cannot be null");
        conf.setSubscriptionProperties(Collections.unmodifiableMap(subscriptionProperties));
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> isAckReceiptEnabled(boolean isAckReceiptEnabled) {
        conf.setAckReceiptEnabled(isAckReceiptEnabled);
        return this;
    }
    @Override
    public FetchConsumerBuilder<T> subscriptionType(@NonNull SubscriptionType subscriptionType) {
        conf.setSubscriptionType(subscriptionType);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> subscriptionMode(@NonNull SubscriptionMode subscriptionMode) {
        conf.setSubscriptionMode(subscriptionMode);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> consumerEventListener(@NonNull ConsumerEventListener consumerEventListener) {
        conf.setConsumerEventListener(consumerEventListener);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> cryptoKeyReader(@NonNull CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> defaultCryptoKeyReader(String privateKey) {
        checkArgument(StringUtils.isNotBlank(privateKey), "privateKey cannot be blank");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().defaultPrivateKey(privateKey).build());
    }

    @Override
    public FetchConsumerBuilder<T> defaultCryptoKeyReader(@NonNull Map<String, String> privateKeys) {
        checkArgument(!privateKeys.isEmpty(), "privateKeys cannot be empty");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().privateKeys(privateKeys).build());
    }

    @Override
    public FetchConsumerBuilder<T> messageCrypto(@NonNull MessageCrypto messageCrypto) {
        conf.setMessageCrypto(messageCrypto);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> cryptoFailureAction(@NonNull ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit) {
        checkArgument(delay >= 0, "acknowledgmentGroupTime needs to be >= 0");
        conf.setAcknowledgementsGroupTimeMicros(unit.toMicros(delay));
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> maxAcknowledgmentGroupSize(int messageNum) {
        checkArgument(messageNum > 0, "acknowledgementsGroupSize needs to be > 0");
        conf.setMaxAcknowledgmentGroupSize(messageNum);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> consumerName(String consumerName) {
        checkArgument(StringUtils.isNotBlank(consumerName), "consumerName cannot be blank");
        conf.setConsumerName(consumerName);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> maxPendingChuckedMessage(int maxPendingChuckedMessage) {
        conf.setMaxPendingChunkedMessage(maxPendingChuckedMessage);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> maxPendingChunkedMessage(int maxPendingChunkedMessage) {
        conf.setMaxPendingChunkedMessage(maxPendingChunkedMessage);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> autoAckOldestChunkedMessageOnQueueFull(
            boolean autoAckOldestChunkedMessageOnQueueFull) {
        conf.setAutoAckOldestChunkedMessageOnQueueFull(autoAckOldestChunkedMessageOnQueueFull);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> property(String key, String value) {
        checkArgument(StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value),
                "property key/value cannot be blank");
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> properties(@NonNull Map<String, String> properties) {
        properties.entrySet().forEach(entry ->
                checkArgument(
                        StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue()),
                        "properties' key/value cannot be blank"));
        conf.getProperties().putAll(properties);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> readCompacted(boolean readCompacted) {
        conf.setReadCompacted(readCompacted);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> subscriptionInitialPosition(@NonNull SubscriptionInitialPosition
                                                                  subscriptionInitialPosition) {
        conf.setSubscriptionInitialPosition(subscriptionInitialPosition);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> replicateSubscriptionState(boolean replicateSubscriptionState) {
        conf.setReplicateSubscriptionState(replicateSubscriptionState);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> intercept(ConsumerInterceptor<T>... interceptors) {
        if (interceptorList == null) {
            interceptorList = new ArrayList<>();
        }
        interceptorList.addAll(Arrays.asList(interceptors));
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy) {
        if (deadLetterPolicy != null) {
            checkArgument(deadLetterPolicy.getMaxRedeliverCount() > 0, "MaxRedeliverCount must be > 0.");
        }
        conf.setDeadLetterPolicy(deadLetterPolicy);
        return this;
    }

    public FetchConsumerBuilder<T> batchReceivePolicy(BatchReceivePolicy batchReceivePolicy) {
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
    public FetchConsumerBuilder<T> enableRetry(boolean retryEnable) {
        conf.setRetryEnable(retryEnable);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> enableBatchIndexAcknowledgment(boolean batchIndexAcknowledgmentEnabled) {
        conf.setBatchIndexAckEnabled(batchIndexAcknowledgmentEnabled);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> expireTimeOfIncompleteChunkedMessage(long duration, TimeUnit unit) {
        conf.setExpireTimeOfIncompleteChunkedMessageMillis(unit.toMillis(duration));
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> poolMessages(boolean poolMessages) {
        conf.setPoolMessages(poolMessages);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> messagePayloadProcessor(MessagePayloadProcessor payloadProcessor) {
        conf.setPayloadProcessor(payloadProcessor);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> negativeAckRedeliveryBackoff(RedeliveryBackoff negativeAckRedeliveryBackoff) {
        checkArgument(negativeAckRedeliveryBackoff != null, "negativeAckRedeliveryBackoff must not be null.");
        conf.setNegativeAckRedeliveryBackoff(negativeAckRedeliveryBackoff);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> startPaused(boolean paused) {
        conf.setStartPaused(paused);
        return this;
    }

    @Override
    public FetchConsumerBuilder<T> clone(){
        return new FetchConsumerBuilderImpl<>(client, conf.clone(), schema);
    }

}
