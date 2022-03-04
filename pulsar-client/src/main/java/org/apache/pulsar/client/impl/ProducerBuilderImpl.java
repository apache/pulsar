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
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptorWrapper;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

@Getter(AccessLevel.PUBLIC)
public class ProducerBuilderImpl<T> implements ProducerBuilder<T> {

    private final PulsarClientImpl client;
    private ProducerConfigurationData conf;
    private Schema<T> schema;
    private List<ProducerInterceptor> interceptorList;

    public ProducerBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
        this(client, new ProducerConfigurationData(), schema);
    }

    private ProducerBuilderImpl(PulsarClientImpl client, ProducerConfigurationData conf, Schema<T> schema) {
        this.client = client;
        this.conf = conf;
        this.schema = schema;
    }

    /**
     * Allow to override schema in builder implementation.
     * @return
     */
    public ProducerBuilder<T> schema(Schema<T> schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public ProducerBuilder<T> clone() {
        return new ProducerBuilderImpl<>(client, conf.clone(), schema);
    }

    @Override
    public Producer<T> create() throws PulsarClientException {
        try {
            return createAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Producer<T>> createAsync() {
        // config validation
        checkArgument(!(conf.isBatchingEnabled() && conf.isChunkingEnabled()),
                "Batching and chunking of messages can't be enabled together");
        if (conf.getTopicName() == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic name must be set on the producer builder"));
        }

        try {
            setMessageRoutingMode();
        } catch (PulsarClientException pce) {
            return FutureUtil.failedFuture(pce);
        }

        return interceptorList == null || interceptorList.size() == 0
                ? client.createProducerAsync(conf, schema, null)
                : client.createProducerAsync(conf, schema, new ProducerInterceptors(interceptorList));
    }

    @Override
    public ProducerBuilder<T> loadConf(Map<String, Object> config) {
        conf = ConfigurationDataUtils.loadData(
            config, conf, ProducerConfigurationData.class);
        return this;
    }

    @Override
    public ProducerBuilder<T> topic(String topicName) {
        checkArgument(StringUtils.isNotBlank(topicName), "topicName cannot be blank");
        conf.setTopicName(StringUtils.trim(topicName));
        return this;
    }

    @Override
    public ProducerBuilder<T> producerName(String producerName) {
        conf.setProducerName(producerName);
        return this;
    }

    @Override
    public ProducerBuilder<T> sendTimeout(int sendTimeout, @NonNull TimeUnit unit) {
        conf.setSendTimeoutMs(sendTimeout, unit);
        return this;
    }

    @Override
    public ProducerBuilder<T> maxPendingMessages(int maxPendingMessages) {
        conf.setMaxPendingMessages(maxPendingMessages);
        return this;
    }

    @Deprecated
    @Override
    public ProducerBuilder<T> maxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
        conf.setMaxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions);
        return this;
    }

    @Override
    public ProducerBuilder<T> accessMode(ProducerAccessMode accessMode) {
        conf.setAccessMode(accessMode);
        return this;
    }

    @Override
    public ProducerBuilder<T> blockIfQueueFull(boolean blockIfQueueFull) {
        conf.setBlockIfQueueFull(blockIfQueueFull);
        return this;
    }

    @Override
    public ProducerBuilder<T> messageRoutingMode(@NonNull MessageRoutingMode messageRouteMode) {
        conf.setMessageRoutingMode(messageRouteMode);
        return this;
    }

    @Override
    public ProducerBuilder<T> compressionType(@NonNull CompressionType compressionType) {
        conf.setCompressionType(compressionType);
        return this;
    }

    @Override
    public ProducerBuilder<T> hashingScheme(@NonNull HashingScheme hashingScheme) {
        conf.setHashingScheme(hashingScheme);
        return this;
    }

    @Override
    public ProducerBuilder<T> messageRouter(@NonNull MessageRouter messageRouter) {
        conf.setCustomMessageRouter(messageRouter);
        return this;
    }

    @Override
    public ProducerBuilder<T> enableBatching(boolean batchMessagesEnabled) {
        conf.setBatchingEnabled(batchMessagesEnabled);
        return this;
    }

    @Override
    public ProducerBuilder<T> enableChunking(boolean chunkingEnabled) {
        conf.setChunkingEnabled(chunkingEnabled);
        return this;
    }

    @Override
    public ProducerBuilder<T> cryptoKeyReader(@NonNull CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ProducerBuilder<T> defaultCryptoKeyReader(String publicKey) {
        checkArgument(StringUtils.isNotBlank(publicKey), "publicKey cannot be blank");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().defaultPublicKey(publicKey).build());
    }

    @Override
    public ProducerBuilder<T> defaultCryptoKeyReader(@NonNull Map<String, String> publicKeys) {
        checkArgument(!publicKeys.isEmpty(), "publicKeys cannot be empty");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().publicKeys(publicKeys).build());
    }

    @Override
    public ProducerBuilder<T> addEncryptionKey(String key) {
        checkArgument(StringUtils.isNotBlank(key), "Encryption key cannot be blank");
        conf.getEncryptionKeys().add(key);
        return this;
    }

    @Override
    public ProducerBuilder<T> cryptoFailureAction(@NonNull ProducerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public ProducerBuilder<T> batchingMaxPublishDelay(long batchDelay, @NonNull TimeUnit timeUnit) {
        conf.setBatchingMaxPublishDelayMicros(batchDelay, timeUnit);
        return this;
    }

    @Override
    public ProducerBuilder<T> roundRobinRouterBatchingPartitionSwitchFrequency(int frequency) {
        conf.setBatchingPartitionSwitchFrequencyByPublishDelay(frequency);
        return this;
    }

    @Override
    public ProducerBuilder<T> batchingMaxMessages(int batchMessagesMaxMessagesPerBatch) {
        conf.setBatchingMaxMessages(batchMessagesMaxMessagesPerBatch);
        return this;
    }

    @Override
    public ProducerBuilder<T> batchingMaxBytes(int batchingMaxBytes) {
        conf.setBatchingMaxBytes(batchingMaxBytes);
        return this;
    }

    @Override
    public ProducerBuilder<T> batcherBuilder(BatcherBuilder batcherBuilder) {
        conf.setBatcherBuilder(batcherBuilder);
        return this;
    }


    @Override
    public ProducerBuilder<T> initialSequenceId(long initialSequenceId) {
        conf.setInitialSequenceId(initialSequenceId);
        return this;
    }

    @Override
    public ProducerBuilder<T> property(String key, String value) {
        checkArgument(StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value),
                "property key/value cannot be blank");
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public ProducerBuilder<T> properties(@NonNull Map<String, String> properties) {
        properties.entrySet().forEach(entry ->
            checkArgument(StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue()),
                    "properties' key/value cannot be blank"));
        conf.getProperties().putAll(properties);
        return this;
    }

    @Override
    public ProducerBuilder<T> intercept(ProducerInterceptor... interceptors) {
        if (interceptorList == null) {
            interceptorList = new ArrayList<>();
        }
        interceptorList.addAll(Arrays.asList(interceptors));
        return this;
    }

    @Override
    @Deprecated
    public ProducerBuilder<T> intercept(org.apache.pulsar.client.api.ProducerInterceptor<T>... interceptors) {
        if (interceptorList == null) {
            interceptorList = new ArrayList<>();
        }
        interceptorList.addAll(Arrays.stream(interceptors).map(ProducerInterceptorWrapper::new)
                                     .collect(Collectors.toList()));
        return this;
    }
    @Override
    public ProducerBuilder<T> autoUpdatePartitions(boolean autoUpdate) {
        conf.setAutoUpdatePartitions(autoUpdate);
        return this;
    }

    @Override
    public ProducerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit) {
        conf.setAutoUpdatePartitionsIntervalSeconds(interval, unit);
        return this;
    }

    @Override
    public ProducerBuilder<T> enableMultiSchema(boolean multiSchema) {
        conf.setMultiSchema(multiSchema);
        return this;
    }

    @Override
    public ProducerBuilder<T> enableLazyStartPartitionedProducers(boolean lazyStartPartitionedProducers) {
        conf.setLazyStartPartitionedProducers(lazyStartPartitionedProducers);
        return this;
    }

    /**
     * Use this config to automatically create an initial subscription when creating the topic.
     * If this field is not set, the initial subscription will not be created.
     * If this field is set but the broker's `allowAutoSubscriptionCreation` is disabled, the producer will fail to
     * be created.
     * This method is limited to internal use. This method will only be used when the consumer creates the dlq producer.
     *
     * @param initialSubscriptionName Name of the initial subscription of the topic.
     * @return the producer builder implementation instance
     */
    public ProducerBuilderImpl<T> initialSubscriptionName(String initialSubscriptionName) {
        conf.setInitialSubscriptionName(initialSubscriptionName);
        return this;
    }

    private void setMessageRoutingMode() throws PulsarClientException {
        if (conf.getMessageRoutingMode() == null && conf.getCustomMessageRouter() == null) {
            messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        } else if (conf.getMessageRoutingMode() == null && conf.getCustomMessageRouter() != null) {
            messageRoutingMode(MessageRoutingMode.CustomPartition);
        } else if ((conf.getMessageRoutingMode() == MessageRoutingMode.CustomPartition
                && conf.getCustomMessageRouter() == null)
                || (conf.getMessageRoutingMode() != MessageRoutingMode.CustomPartition
                && conf.getCustomMessageRouter() != null)) {
            throw new PulsarClientException("When 'messageRouter' is set, 'messageRoutingMode' "
                    + "should be set as " + MessageRoutingMode.CustomPartition);
        }
    }

    @Override
    public String toString() {
        return conf != null ? conf.toString() : "";
    }
}
