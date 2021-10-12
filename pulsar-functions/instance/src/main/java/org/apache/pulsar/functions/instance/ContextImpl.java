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
package org.apache.pulsar.functions.instance;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.prometheus.client.Summary;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.StateStore;
import org.apache.pulsar.functions.instance.state.DefaultStateStore;
import org.apache.pulsar.functions.instance.state.StateManager;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.instance.stats.FunctionStatsManager;
import org.apache.pulsar.functions.instance.stats.SinkStatsManager;
import org.apache.pulsar.functions.instance.stats.SourceStatsManager;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.pulsar.functions.instance.stats.FunctionStatsManager.USER_METRIC_PREFIX;

/**
 * This class implements the Context interface exposed to the user.
 */
@ToString(exclude = {"pulsarAdmin"})
class ContextImpl implements Context, SinkContext, SourceContext, AutoCloseable {
    private InstanceConfig config;
    private Logger logger;

    // Per Message related
    private Record<?> record;

    private final ClientBuilder clientBuilder;
    private final PulsarClient client;
    private final PulsarAdmin pulsarAdmin;
    private Map<String, Producer<?>> publishProducers;
    private ThreadLocal<Map<String, Producer<?>>> tlPublishProducers;
    private ProducerBuilderImpl<?> producerBuilder;

    private final TopicSchema topicSchema;

    private final SecretsProvider secretsProvider;
    private final Map<String, Object> secretsMap;

    @VisibleForTesting
    StateManager stateManager;
    @VisibleForTesting
    DefaultStateStore defaultStateStore;

    private Map<String, Object> userConfigs;

    private ComponentStatsManager statsManager;

    Map<String, String[]> userMetricsLabels = new HashMap<>();
    private final String[] metricsLabels;
    private final Summary userMetricsSummary;

    private final SubscriptionType subscriptionType;

    private static final String[] userMetricsLabelNames;

    private boolean exposePulsarAdminClientEnabled;

    private List<Consumer<?>> inputConsumers;
    private final Map<TopicName, Consumer> topicConsumers = new ConcurrentHashMap<>();

    static {
        // add label to indicate user metric
        userMetricsLabelNames = Arrays.copyOf(ComponentStatsManager.metricsLabelNames, ComponentStatsManager.metricsLabelNames.length + 1);
        userMetricsLabelNames[ComponentStatsManager.metricsLabelNames.length] = "metric";
    }

    private final Function.FunctionDetails.ComponentType componentType;

    public ContextImpl(InstanceConfig config, Logger logger, PulsarClient client,
                       SecretsProvider secretsProvider, FunctionCollectorRegistry collectorRegistry, String[] metricsLabels,
                       Function.FunctionDetails.ComponentType componentType, ComponentStatsManager statsManager,
                       StateManager stateManager, PulsarAdmin pulsarAdmin, ClientBuilder clientBuilder) throws PulsarClientException {
        this.config = config;
        this.logger = logger;
        this.clientBuilder = clientBuilder;
        this.client = client;
        this.pulsarAdmin = pulsarAdmin;
        this.topicSchema = new TopicSchema(client);
        this.statsManager = statsManager;

        this.producerBuilder = (ProducerBuilderImpl<?>) client.newProducer().blockIfQueueFull(true).enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
        boolean useThreadLocalProducers = false;
        Function.ProducerSpec producerSpec = config.getFunctionDetails().getSink().getProducerSpec();
        if (producerSpec != null) {
            if (producerSpec.getMaxPendingMessages() != 0) {
                this.producerBuilder.maxPendingMessages(producerSpec.getMaxPendingMessages());
            }
            if (producerSpec.getMaxPendingMessagesAcrossPartitions() != 0) {
                this.producerBuilder.maxPendingMessagesAcrossPartitions(producerSpec.getMaxPendingMessagesAcrossPartitions());
            }
            if (producerSpec.getBatchBuilder() != null) {
                if (producerSpec.getBatchBuilder().equals("KEY_BASED")) {
                    this.producerBuilder.batcherBuilder(BatcherBuilder.KEY_BASED);
                } else {
                    this.producerBuilder.batcherBuilder(BatcherBuilder.DEFAULT);
                }
            }
            useThreadLocalProducers = producerSpec.getUseThreadLocalProducers();
        }
        if (useThreadLocalProducers) {
            tlPublishProducers = new ThreadLocal<>();
        } else {
            publishProducers = new ConcurrentHashMap<>();
        }

        if (config.getFunctionDetails().getUserConfig().isEmpty()) {
            userConfigs = new HashMap<>();
        } else {
            userConfigs = new Gson().fromJson(config.getFunctionDetails().getUserConfig(),
                    new TypeToken<Map<String, Object>>() {
                    }.getType());
        }
        this.secretsProvider = secretsProvider;
        if (!StringUtils.isEmpty(config.getFunctionDetails().getSecretsMap())) {
            secretsMap = new Gson().fromJson(config.getFunctionDetails().getSecretsMap(),
                    new TypeToken<Map<String, Object>>() {
                    }.getType());
        } else {
            secretsMap = new HashMap<>();
        }

        this.metricsLabels = metricsLabels;
        String prefix;
        switch (componentType) {
            case FUNCTION:
                prefix = FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX;
                break;
            case SINK:
                prefix = SinkStatsManager.PULSAR_SINK_METRICS_PREFIX;
                break;
            case SOURCE:
                prefix = SourceStatsManager.PULSAR_SOURCE_METRICS_PREFIX;
                break;
            default:
                throw new RuntimeException("Unknown component type: " + componentType);
        }
        this.userMetricsSummary = collectorRegistry.registerIfNotExist(
                prefix + ComponentStatsManager.USER_METRIC_PREFIX,
                Summary.build()
                        .name(prefix + ComponentStatsManager.USER_METRIC_PREFIX)
                        .help("User defined metric.")
                        .labelNames(userMetricsLabelNames)
                        .quantile(0.5, 0.01)
                        .quantile(0.9, 0.01)
                        .quantile(0.99, 0.01)
                        .quantile(0.999, 0.01)
                        .create());
        this.componentType = componentType;
        this.stateManager = stateManager;
        this.defaultStateStore = (DefaultStateStore) stateManager.getStore(
            config.getFunctionDetails().getTenant(),
            config.getFunctionDetails().getNamespace(),
            config.getFunctionDetails().getName()
        );
        this.exposePulsarAdminClientEnabled = config.isExposePulsarAdminClientEnabled();

        Function.SourceSpec sourceSpec = config.getFunctionDetails().getSource();
        switch (sourceSpec.getSubscriptionType()) {
            case FAILOVER:
                subscriptionType = SubscriptionType.Failover;
                break;
            case KEY_SHARED:
                subscriptionType = SubscriptionType.Key_Shared;
                break;
            default:
                subscriptionType = SubscriptionType.Shared;
                break;
        }
    }

    public void setCurrentMessageContext(Record<?> record) {
        this.record = record;
    }

    @Override
    public Record<?> getCurrentRecord() {
        return record;
    }

    @Override
    public Collection<String> getInputTopics() {
        return config.getFunctionDetails().getSource().getInputSpecsMap().keySet();
    }

    @Override
    public String getOutputTopic() {
        return config.getFunctionDetails().getSink().getTopic();
    }

    @Override
    public String getOutputSchemaType() {
        SinkSpec sink = config.getFunctionDetails().getSink();
        if (!StringUtils.isEmpty(sink.getSchemaType())) {
            return sink.getSchemaType();
        } else {
            return sink.getSerDeClassName();
        }
    }

    @Override
    public String getTenant() {
        return config.getFunctionDetails().getTenant();
    }

    @Override
    public String getNamespace() {
        return config.getFunctionDetails().getNamespace();
    }

    @Override
    public String getSinkName() {
        return config.getFunctionDetails().getName();
    }

    @Override
    public String getSourceName() {
        return config.getFunctionDetails().getName();
    }

    @Override
    public String getFunctionName() {
        return config.getFunctionDetails().getName();
    }

    @Override
    public String getFunctionId() {
        return config.getFunctionId();
    }

    @Override
    public int getInstanceId() {
        return config.getInstanceId();
    }

    @Override
    public int getNumInstances() {
        return config.getFunctionDetails().getParallelism();
    }

    @Override
    public String getFunctionVersion() {
        return config.getFunctionVersion();
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public Optional<Object> getUserConfigValue(String key) {
        Object value = userConfigs.getOrDefault(key, null);

        if (value instanceof String && ((String) value).startsWith("$")) {
            // any string starts with '$' is considered as system env symbol and will be
            // replaced with the actual env value
            try {
                String actualValue = System.getenv(((String) value).substring(1));
                return Optional.ofNullable(actualValue);
            } catch (SecurityException ex) {
                throw new RuntimeException("Access to environment variable " + value + " is not allowed.", ex);
            }
        }  else {
            return Optional.ofNullable(value);
        }
    }

    @Override
    public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
        return getUserConfigValue(key).orElse(defaultValue);
    }

    @Override
    public Map<String, Object> getUserConfigMap() {
        return userConfigs;
    }

    @Override
    public String getSecret(String secretName) {
        if (secretsMap.containsKey(secretName)) {
            return secretsProvider.provideSecret(secretName, secretsMap.get(secretName));
        } else {
            return null;
        }
    }

    @Override
    public PulsarAdmin getPulsarAdmin() {
        if (exposePulsarAdminClientEnabled) {
            return pulsarAdmin;
        } else {
            throw new IllegalStateException("PulsarAdmin is not enabled in function worker");
        }
    }

    @Override
    public <S extends StateStore> S getStateStore(String name) {
        return getStateStore(
            config.getFunctionDetails().getTenant(),
            config.getFunctionDetails().getNamespace(),
            name);
    }

    @Override
    public <S extends StateStore> S getStateStore(String tenant, String ns, String name) {
        return (S) stateManager.getStore(tenant, ns, name);
    }

    private void ensureStateEnabled() {
        checkState(null != defaultStateStore, "State %s/%s/%s is not enabled.",
            config.getFunctionDetails().getTenant(),
            config.getFunctionDetails().getNamespace(),
            config.getFunctionDetails().getName());
    }

    @Override
    public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
        ensureStateEnabled();
        return defaultStateStore.incrCounterAsync(key, amount);
    }

    @Override
    public void incrCounter(String key, long amount) {
        ensureStateEnabled();
        defaultStateStore.incrCounter(key, amount);
    }

    @Override
    public CompletableFuture<Long> getCounterAsync(String key) {
        ensureStateEnabled();
        return defaultStateStore.getCounterAsync(key);
    }

    @Override
    public long getCounter(String key) {
        ensureStateEnabled();
        return defaultStateStore.getCounter(key);
    }

    @Override
    public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
        ensureStateEnabled();
        return defaultStateStore.putAsync(key, value);
    }

    @Override
    public void putState(String key, ByteBuffer value) {
        ensureStateEnabled();
        defaultStateStore.put(key, value);
    }

    @Override
    public CompletableFuture<Void> deleteStateAsync(String key) {
        ensureStateEnabled();
        return defaultStateStore.deleteAsync(key);
    }

    @Override
    public void deleteState(String key) {
        ensureStateEnabled();
        defaultStateStore.delete(key);
    }

    @Override
    public CompletableFuture<ByteBuffer> getStateAsync(String key) {
        ensureStateEnabled();
        return defaultStateStore.getAsync(key);
    }

    @Override
    public ByteBuffer getState(String key) {
        ensureStateEnabled();
        return defaultStateStore.get(key);
    }

    @Override
    public <O> CompletableFuture<Void> publish(String topicName, O object) {
        return publish(topicName, object, "");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O> CompletableFuture<Void> publish(String topicName, O object, String schemaOrSerdeClassName) {
        return publish(topicName, object, (Schema<O>) topicSchema.getSchema(topicName, object, schemaOrSerdeClassName, false));
    }

    @Override
    public <O> TypedMessageBuilder<O> newOutputMessage(String topicName, Schema<O> schema) throws PulsarClientException {
        MessageBuilderImpl<O> messageBuilder = new MessageBuilderImpl<>();
        TypedMessageBuilder<O> typedMessageBuilder;
        Producer<O> producer = getProducer(topicName, schema);
        if (schema != null) {
            typedMessageBuilder = producer.newMessage(schema);
        } else {
            typedMessageBuilder = producer.newMessage();
        }
        messageBuilder.setUnderlyingBuilder(typedMessageBuilder);
        return messageBuilder;
    }

    @Override
    public <O> ConsumerBuilder<O> newConsumerBuilder(Schema<O> schema) throws PulsarClientException {
        return this.client.newConsumer(schema);
    }

    @Override
    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    public <O> CompletableFuture<Void> publish(String topicName, O object, Schema<O> schema) {
        try {
            return newOutputMessage(topicName, schema).value(object).sendAsync().thenApply(msgId -> null);
        } catch (PulsarClientException e) {
            logger.error("Failed to create Producer while doing user publish", e);
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public void recordMetric(String metricName, double value) {
        String[] userMetricLabels = userMetricsLabels.get(metricName);
        if (userMetricLabels == null) {
            userMetricLabels = Arrays.copyOf(metricsLabels, metricsLabels.length + 1);
            userMetricLabels[userMetricLabels.length - 1] = metricName;
            // set label for metrics before putting into userMetricsLabels map to
            // prevent race condition with getMetrics calls
            userMetricsSummary.labels(userMetricLabels).observe(value);
            userMetricsLabels.put(metricName, userMetricLabels);
        } else {
            userMetricsSummary.labels(userMetricLabels).observe(value);
        }
    }

    @Override
    public PulsarClient getPulsarClient() {
        return client;
    }

    @Override
    public ClientBuilder getPulsarClientBuilder() {
        return clientBuilder;
    }

    private <O> Producer<O> getProducer(String topicName, Schema<O> schema) throws PulsarClientException {
        Producer<O> producer;
        if (tlPublishProducers != null) {
            Map<String, Producer<?>> producerMap = tlPublishProducers.get();
            if (producerMap == null) {
                producerMap = new HashMap<>();
                tlPublishProducers.set(producerMap);
            }
            producer = (Producer<O>) producerMap.get(topicName);
        } else {
            producer = (Producer<O>) publishProducers.get(topicName);
        }

        if (producer == null) {

            Producer<O> newProducer = ((ProducerBuilderImpl<O>) producerBuilder.clone())
                    .schema(schema)
                    .blockIfQueueFull(true)
                    .enableBatching(true)
                    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                    .compressionType(CompressionType.LZ4)
                    .hashingScheme(HashingScheme.Murmur3_32Hash) //
                    .messageRoutingMode(MessageRoutingMode.CustomPartition)
                    .messageRouter(FunctionResultRouter.of())
                    // set send timeout to be infinity to prevent potential deadlock with consumer
                    // that might happen when consumer is blocked due to unacked messages
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .topic(topicName)
                    .properties(InstanceUtils.getProperties(componentType,
                            FunctionCommon.getFullyQualifiedName(
                                    this.config.getFunctionDetails().getTenant(),
                                    this.config.getFunctionDetails().getNamespace(),
                                    this.config.getFunctionDetails().getName()),
                            this.config.getInstanceId()))
                    .create();

            if (tlPublishProducers != null) {
                tlPublishProducers.get().put(topicName, newProducer);
            } else {
                Producer<O> existingProducer = (Producer<O>) publishProducers.putIfAbsent(topicName, newProducer);

                if (existingProducer != null) {
                    // The value in the map was not updated after the concurrent put
                    newProducer.close();
                    producer = existingProducer;
                } else {
                    producer = newProducer;
                }
            }
        }
        return producer;
    }

    public Map<String, Double> getAndResetMetrics() {
        Map<String, Double> retval = getMetrics();
        resetMetrics();
        return retval;
    }

    public void resetMetrics() {
        userMetricsSummary.clear();
    }

    public Map<String, Double> getMetrics() {
        Map<String, Double> metricsMap = new HashMap<>();
        for (Map.Entry<String, String[]> userMetricsLabelsEntry : userMetricsLabels.entrySet()) {
            String metricName = userMetricsLabelsEntry.getKey();
            String[] labels = userMetricsLabelsEntry.getValue();
            Summary.Child.Value summary = userMetricsSummary.labels(labels).get();
            metricsMap.put(String.format("%s%s_sum", USER_METRIC_PREFIX, metricName), summary.sum);
            metricsMap.put(String.format("%s%s_count", USER_METRIC_PREFIX, metricName), summary.count);
            for (Map.Entry<Double, Double> entry : summary.quantiles.entrySet()) {
                Double quantile = entry.getKey();
                Double value = entry.getValue();
                metricsMap.put(String.format("%s%s_%s", USER_METRIC_PREFIX, metricName, quantile), value);
            }
        }
        return metricsMap;
    }

    class MessageBuilderImpl<O> implements TypedMessageBuilder<O> {
        private TypedMessageBuilder<O> underlyingBuilder;
        @Override
        public MessageId send() throws PulsarClientException {
            try {
                return sendAsync().get();
            } catch (Exception e) {
                throw PulsarClientException.unwrap(e);
            }
        }

        @Override
        public CompletableFuture<MessageId> sendAsync() {
            return underlyingBuilder.sendAsync()
                    .whenComplete((result, cause) -> {
                        if (null != cause) {
                            statsManager.incrSysExceptions(cause);
                            logger.error("Failed to publish to topic with error", cause);
                        }
                    });
        }

        @Override
        public TypedMessageBuilder<O> key(String key) {
            underlyingBuilder.key(key);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> keyBytes(byte[] key) {
            underlyingBuilder.keyBytes(key);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> orderingKey(byte[] orderingKey) {
            underlyingBuilder.orderingKey(orderingKey);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> value(O value) {
            underlyingBuilder.value(value);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> property(String name, String value) {
            underlyingBuilder.property(name, value);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> properties(Map<String, String> properties) {
            underlyingBuilder.properties(properties);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> eventTime(long timestamp) {
            underlyingBuilder.eventTime(timestamp);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> sequenceId(long sequenceId) {
            underlyingBuilder.sequenceId(sequenceId);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> replicationClusters(List<String> clusters) {
            underlyingBuilder.replicationClusters(clusters);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> disableReplication() {
            underlyingBuilder.disableReplication();
            return this;
        }

        @Override
        public TypedMessageBuilder<O> loadConf(Map<String, Object> config) {
            underlyingBuilder.loadConf(config);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> deliverAfter(long delay, TimeUnit unit) {
            underlyingBuilder.deliverAfter(delay, unit);
            return this;
        }

        @Override
        public TypedMessageBuilder<O> deliverAt(long timestamp) {
            underlyingBuilder.deliverAt(timestamp);
            return this;
        }

        public void setUnderlyingBuilder(TypedMessageBuilder<O> underlyingBuilder) {
            this.underlyingBuilder = underlyingBuilder;
        }
    }

    @Override
    public void close() {
        List<CompletableFuture> futures = new LinkedList<>();

        if (publishProducers != null) {
            for (Producer<?> producer : publishProducers.values()) {
                futures.add(producer.closeAsync());
            }
        }

        if (tlPublishProducers != null) {
            for (Producer<?> producer : tlPublishProducers.get().values()) {
                futures.add(producer.closeAsync());
            }
        }

        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Failed to close producers", e);
        }
    }

    @Override
    public void seek(String topic, int partition, MessageId messageId) throws PulsarClientException {
        Consumer<?> consumer = getConsumer(topic, partition);
        consumer.seek(messageId);
    }

    @Override
    public void pause(String topic, int partition) throws PulsarClientException {
        getConsumer(topic, partition).pause();
    }

    @Override
    public void resume(String topic, int partition) throws PulsarClientException {
        getConsumer(topic, partition).resume();
    }

    public void setInputConsumers(List<Consumer<?>> inputConsumers) {
        this.inputConsumers = inputConsumers;
        inputConsumers.stream()
            .flatMap(consumer ->
                    consumer instanceof MultiTopicsConsumerImpl
                            ? ((MultiTopicsConsumerImpl<?>) consumer).getConsumers().stream()
                            : Stream.of(consumer))
            .forEach(consumer -> topicConsumers.putIfAbsent(TopicName.get(consumer.getTopic()), consumer));
    }

    private void reloadConsumersFromMultiTopicsConsumers() {
        // MultiTopicsConsumer in the list of inputConsumers could change its nested consumers
        // if ne partition was created or a new topic added that matches subscription pattern.
        // Let's update topicConsumers map to match.
        inputConsumers
                .stream()
                .flatMap(c ->
                        c instanceof MultiTopicsConsumerImpl
                                ? ((MultiTopicsConsumerImpl<?>) c).getConsumers().stream()
                                : Stream.empty() // no changes expected in regular consumers
                ).forEach(c -> topicConsumers.putIfAbsent(TopicName.get(c.getTopic()), c));
    }

    // returns null if consumer not found
    private Consumer<?> tryGetConsumer(String topic, int partition) {
        if (partition == 0) {
            // maybe a non-partitioned topic
            Consumer<?> consumer = topicConsumers.get(TopicName.get(topic));

            if (consumer != null) {
                return consumer;
            }
        }
        // maybe partitioned topic
        return topicConsumers.get(TopicName.get(topic).getPartition(partition));
    }

    @VisibleForTesting
    Consumer<?> getConsumer(String topic, int partition) throws PulsarClientException {
        if (inputConsumers == null) {
            throw new PulsarClientException("Getting consumer is not supported");
        }

        Consumer<?> consumer = tryGetConsumer(topic, partition);
        if (consumer == null) {
            // MultiTopicsConsumer's list of consumers could change
            // if partitions changed or pattern(s) used to subscribe.
            // Reload and try one more time.
            reloadConsumersFromMultiTopicsConsumers();
            consumer = tryGetConsumer(topic, partition);
        }

        if (consumer != null) {
            return consumer;
        }
        throw new PulsarClientException("Consumer for topic " + topic
                + " partition " + partition + " is not found");
    }
}
