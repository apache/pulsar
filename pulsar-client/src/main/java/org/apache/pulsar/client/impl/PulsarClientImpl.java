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

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.client.impl.transaction.TransactionBuilderImpl;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.BackoffBuilder;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarClientImpl implements PulsarClient {

    private static final Logger log = LoggerFactory.getLogger(PulsarClientImpl.class);
    private static final int CLOSE_TIMEOUT_SECONDS = 60;
    private static final double THRESHOLD_FOR_CONSUMER_RECEIVER_QUEUE_SIZE_SHRINKING = 0.95;

    // default limits for producers when memory limit controller is disabled
    private static final int NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES = 1000;
    private static final int NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS = 50000;

    protected final ClientConfigurationData conf;
    private final boolean createdExecutorProviders;

    private final boolean createdScheduledProviders;
    private final boolean createdLookupProviders;
    private LookupService lookup;
    private Map<String, LookupService> urlLookupMap = new ConcurrentHashMap<>();
    private final ConnectionPool cnxPool;
    @Getter
    private final Timer timer;
    private boolean needStopTimer;
    private final ExecutorProvider externalExecutorProvider;
    private final ExecutorProvider internalExecutorProvider;
    private final ExecutorProvider lookupExecutorProvider;

    private final ScheduledExecutorProvider scheduledExecutorProvider;
    private final boolean createdEventLoopGroup;
    private final boolean createdCnxPool;

    public enum State {
        Open, Closing, Closed
    }

    private final AtomicReference<State> state = new AtomicReference<>();
    // These sets are updated from multiple threads, so they require a threadsafe data structure
    private final Set<ProducerBase<?>> producers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<ConsumerBase<?>> consumers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final AtomicLong producerIdGenerator = new AtomicLong();
    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong topicListWatcherIdGenerator = new AtomicLong();
    private final AtomicLong requestIdGenerator =
            new AtomicLong(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE / 2));

    protected final EventLoopGroup eventLoopGroup;
    private final MemoryLimitController memoryLimitController;

    private final LoadingCache<String, SchemaInfoProvider> schemaProviderLoadingCache =
            CacheBuilder.newBuilder().maximumSize(100000)
                    .expireAfterAccess(30, TimeUnit.MINUTES)
                    .build(new CacheLoader<String, SchemaInfoProvider>() {

                        @Override
                        public SchemaInfoProvider load(String topicName) {
                            return newSchemaProvider(topicName);
                        }
                    });

    private final Clock clientClock;

    @Getter
    private TransactionCoordinatorClientImpl tcClient;

    public PulsarClientImpl(ClientConfigurationData conf) throws PulsarClientException {
        this(conf, null, null, null, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, null, null, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, null, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool,
                            Timer timer)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, timer, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool connectionPool,
                            Timer timer, ExecutorProvider externalExecutorProvider,
                            ExecutorProvider internalExecutorProvider,
                            ScheduledExecutorProvider scheduledExecutorProvider)
            throws PulsarClientException {
        this(conf, eventLoopGroup, connectionPool, timer, externalExecutorProvider, internalExecutorProvider,
                scheduledExecutorProvider, null);
    }

    @Builder(builderClassName = "PulsarClientImplBuilder")
    private PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool connectionPool,
                             Timer timer, ExecutorProvider externalExecutorProvider,
                             ExecutorProvider internalExecutorProvider,
                             ScheduledExecutorProvider scheduledExecutorProvider,
                             ExecutorProvider lookupExecutorProvider) throws PulsarClientException {

        EventLoopGroup eventLoopGroupReference = null;
        ConnectionPool connectionPoolReference = null;
        try {
            this.createdEventLoopGroup = eventLoopGroup == null;
            this.createdCnxPool = connectionPool == null;
            if ((externalExecutorProvider == null) != (internalExecutorProvider == null)) {
                throw new IllegalArgumentException(
                        "Both externalExecutorProvider and internalExecutorProvider must be specified or unspecified.");
            }
            this.createdExecutorProviders = externalExecutorProvider == null;
            this.createdScheduledProviders = scheduledExecutorProvider == null;
            this.createdLookupProviders = lookupExecutorProvider == null;
            eventLoopGroupReference = eventLoopGroup != null ? eventLoopGroup : getEventLoopGroup(conf);
            this.eventLoopGroup = eventLoopGroupReference;
            if (conf == null || isBlank(conf.getServiceUrl())) {
                throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
            }
            this.conf = conf;
            clientClock = conf.getClock();
            conf.getAuthentication().start();
            connectionPoolReference =
                    connectionPool != null ? connectionPool : new ConnectionPool(conf, this.eventLoopGroup);
            this.cnxPool = connectionPoolReference;
            this.externalExecutorProvider = externalExecutorProvider != null ? externalExecutorProvider :
                    new ExecutorProvider(conf.getNumListenerThreads(), "pulsar-external-listener");
            this.internalExecutorProvider = internalExecutorProvider != null ? internalExecutorProvider :
                    new ExecutorProvider(conf.getNumIoThreads(), "pulsar-client-internal");
            this.lookupExecutorProvider = lookupExecutorProvider != null ? lookupExecutorProvider :
                    new ExecutorProvider(1, "pulsar-client-lookup");
            this.scheduledExecutorProvider = scheduledExecutorProvider != null ? scheduledExecutorProvider :
                    new ScheduledExecutorProvider(conf.getNumIoThreads(), "pulsar-client-scheduled");
            if (conf.getServiceUrl().startsWith("http")) {
                lookup = new HttpLookupService(conf, this.eventLoopGroup);
            } else {
                lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.getListenerName(),
                        conf.isUseTls(), this.scheduledExecutorProvider.getExecutor(),
                        this.lookupExecutorProvider.getExecutor());
            }
            if (timer == null) {
                this.timer = new HashedWheelTimer(getThreadFactory("pulsar-timer"), 1, TimeUnit.MILLISECONDS);
                needStopTimer = true;
            } else {
                this.timer = timer;
            }

            if (conf.getServiceUrlProvider() != null) {
                conf.getServiceUrlProvider().initialize(this);
            }

            if (conf.isEnableTransaction()) {
                tcClient = new TransactionCoordinatorClientImpl(this);
                try {
                    tcClient.start();
                } catch (Throwable e) {
                    log.error("Start transactionCoordinatorClient error.", e);
                    throw new PulsarClientException(e);
                }
            }

            memoryLimitController = new MemoryLimitController(conf.getMemoryLimitBytes(),
                    (long) (conf.getMemoryLimitBytes() * THRESHOLD_FOR_CONSUMER_RECEIVER_QUEUE_SIZE_SHRINKING),
                    this::reduceConsumerReceiverQueueSize);
            state.set(State.Open);
        } catch (Throwable t) {
            // Log the exception first, or it could be missed if there are any subsequent exceptions in the
            // shutdown sequence
            log.error("Failed to create Pulsar client instance.", t);
            shutdown();
            shutdownEventLoopGroup(eventLoopGroupReference);
            closeCnxPool(connectionPoolReference);
            throw t;
        }
    }

    private void reduceConsumerReceiverQueueSize() {
        for (ConsumerBase<?> consumer : consumers) {
            consumer.reduceCurrentReceiverQueueSize();
        }
    }

    public ClientConfigurationData getConfiguration() {
        return conf;
    }

    public Clock getClientClock() {
        return clientClock;
    }

    public AtomicReference<State> getState() {
        return state;
    }

    @Override
    public ProducerBuilder<byte[]> newProducer() {
        return new ProducerBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
        ProducerBuilderImpl<T> producerBuilder = new ProducerBuilderImpl<>(this, schema);
        if (!memoryLimitController.isMemoryLimited()) {
            // set default limits for producers when memory limit controller is disabled
            producerBuilder.maxPendingMessages(NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES);
            producerBuilder.maxPendingMessagesAcrossPartitions(
                    NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS);
        }
        return producerBuilder;
    }

    @Override
    public ConsumerBuilder<byte[]> newConsumer() {
        return new ConsumerBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
        return new ConsumerBuilderImpl<>(this, schema);
    }

    @Override
    public ReaderBuilder<byte[]> newReader() {
        return new ReaderBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
        return new ReaderBuilderImpl<>(this, schema);
    }

    /**
     * @deprecated use {@link #newTableView(Schema)} instead.
     */
    @Override
    @Deprecated
    public <T> TableViewBuilder<T> newTableViewBuilder(Schema<T> schema) {
        return new TableViewBuilderImpl<>(this, schema);
    }

    @Override
    public TableViewBuilder<byte[]> newTableView() {
        return new TableViewBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> TableViewBuilder<T> newTableView(Schema<T> schema) {
        return new TableViewBuilderImpl<>(this, schema);
    }

    public CompletableFuture<Producer<byte[]>> createProducerAsync(ProducerConfigurationData conf) {
        return createProducerAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf,  Schema<T> schema) {
        return createProducerAsync(conf, schema, null);
    }

    @SuppressWarnings("rawtypes")
    public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf, Schema<T> schema,
                                                                  ProducerInterceptors interceptors) {
        if (conf == null) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidConfigurationException("Producer configuration undefined"));
        }

        if (schema instanceof AutoConsumeSchema) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException(
                            "AutoConsumeSchema is only used by consumers to detect schemas automatically"));
        }

        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Client already closed : state = " + state.get()));
        }

        String topic = conf.getTopicName();

        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }

        if (schema instanceof AutoProduceBytesSchema) {
            AutoProduceBytesSchema autoProduceBytesSchema = (AutoProduceBytesSchema) schema;
            if (autoProduceBytesSchema.schemaInitialized()) {
                return createProducerAsync(topic, conf, schema, interceptors);
            }
            return lookup.getSchema(TopicName.get(conf.getTopicName()))
                    .thenCompose(schemaInfoOptional -> {
                        if (schemaInfoOptional.isPresent()) {
                            SchemaInfo schemaInfo = schemaInfoOptional.get();
                            if (schemaInfo.getType() == SchemaType.PROTOBUF) {
                                autoProduceBytesSchema.setSchema(new GenericAvroSchema(schemaInfo));
                            } else {
                                autoProduceBytesSchema.setSchema(Schema.getSchema(schemaInfo));
                            }
                        } else {
                            autoProduceBytesSchema.setSchema(Schema.BYTES);
                        }
                        return createProducerAsync(topic, conf, schema, interceptors);
                    });
        } else {
            return createProducerAsync(topic, conf, schema, interceptors);
        }

    }

    private CompletableFuture<Integer> checkPartitions(String topic, boolean forceNoPartitioned,
                                                       @Nullable String producerNameForLog) {
        CompletableFuture<Integer> checkPartitions = new CompletableFuture<>();
        getPartitionedTopicMetadata(topic, !forceNoPartitioned, true).thenAccept(metadata -> {
            if (forceNoPartitioned && metadata.partitions > 0) {
                String errorMsg = String.format("Can not create the producer[%s] for the topic[%s] that contains %s"
                                + " partitions b,ut the producer does not support for a partitioned topic.",
                        producerNameForLog, topic, metadata.partitions);
                log.error(errorMsg);
                checkPartitions.completeExceptionally(
                        new PulsarClientException.LookupException(errorMsg));
            } else {
                checkPartitions.complete(metadata.partitions);
            }
        }).exceptionally(ex -> {
            Throwable actEx = FutureUtil.unwrapCompletionException(ex);
            if (forceNoPartitioned && (actEx instanceof PulsarClientException.NotFoundException
                    || actEx instanceof PulsarClientException.TopicDoesNotExistException
                    || actEx instanceof PulsarAdminException.NotFoundException)) {
                checkPartitions.complete(0);
            } else {
                checkPartitions.completeExceptionally(ex);
            }
            return null;
        });
        return checkPartitions;
    }

    private <T> CompletableFuture<Producer<T>> createProducerAsync(String topic,
                                                                   ProducerConfigurationData conf,
                                                                   Schema<T> schema,
                                                                   ProducerInterceptors interceptors) {
        CompletableFuture<Producer<T>> producerCreatedFuture = new CompletableFuture<>();



        checkPartitions(topic, conf.isNonPartitionedTopicExpected(), conf.getProducerName()).thenAccept(partitions -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, partitions);
            }

            ProducerBase<T> producer;
            if (partitions > 0) {
                producer = newPartitionedProducerImpl(topic, conf, schema, interceptors, producerCreatedFuture,
                        partitions);
            } else {
                producer = newProducerImpl(topic, -1, conf, schema, interceptors, producerCreatedFuture,
                        Optional.empty());
            }
            producers.add(producer);
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata: {}", topic, ex.getMessage());
            producerCreatedFuture.completeExceptionally(ex);
            return null;
        });

        return producerCreatedFuture;
    }

    /**
     * Factory method for creating PartitionedProducerImpl instance.
     *
     * Allows overriding the PartitionedProducerImpl instance in tests.
     *
     * @param topic topic name
     * @param conf producer configuration
     * @param schema topic schema
     * @param interceptors producer interceptors
     * @param producerCreatedFuture future for signaling completion of async producer creation
     * @param <T> message type class
     * @return new PartitionedProducerImpl instance
     */
    protected <T> PartitionedProducerImpl<T> newPartitionedProducerImpl(String topic,
                                                                        ProducerConfigurationData conf,
                                                                        Schema<T> schema,
                                                                        ProducerInterceptors interceptors,
                                                                        CompletableFuture<Producer<T>>
                                                                                producerCreatedFuture,
                                                                        int partitions) {
        return new PartitionedProducerImpl<>(PulsarClientImpl.this, topic, conf, partitions,
                producerCreatedFuture, schema, interceptors);
    }

    /**
     * Factory method for creating ProducerImpl instance.
     *
     * Allows overriding the ProducerImpl instance in tests.
     *
     * @param topic topic name
     * @param partitionIndex partition index of a partitioned topic. the value -1 is used for non-partitioned topics.
     * @param conf producer configuration
     * @param schema topic schema
     * @param interceptors producer interceptors
     * @param producerCreatedFuture future for signaling completion of async producer creation
     * @param <T> message type class
     *
     * @return a producer instance
     */
    protected <T> ProducerImpl<T> newProducerImpl(String topic, int partitionIndex,
                                                  ProducerConfigurationData conf,
                                                  Schema<T> schema,
                                                  ProducerInterceptors interceptors,
                                                  CompletableFuture<Producer<T>> producerCreatedFuture,
                                                  Optional<String> overrideProducerName) {
        return new ProducerImpl<>(PulsarClientImpl.this, topic, conf, producerCreatedFuture, partitionIndex, schema,
                interceptors, overrideProducerName);
    }

    public CompletableFuture<Consumer<byte[]>> subscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return subscribeAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Consumer<T>> subscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema,
                                                             ConsumerInterceptors<T> interceptors) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        for (String topic : conf.getTopicNames()) {
            if (!TopicName.isValid(topic)) {
                return FutureUtil.failedFuture(
                        new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
            }
        }

        if (isBlank(conf.getSubscriptionName())) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Empty subscription name"));
        }

        if (conf.isReadCompacted() && (!conf.getTopicNames().stream()
                .allMatch(topic -> TopicName.get(topic).getDomain() == TopicDomain.persistent)
                || (conf.getSubscriptionType() != SubscriptionType.Exclusive
                        && conf.getSubscriptionType() != SubscriptionType.Failover))) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Read compacted can only be used with exclusive or failover persistent subscriptions"));
        }

        if (conf.getConsumerEventListener() != null && conf.getSubscriptionType() != SubscriptionType.Failover) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Active consumer listener is only supported for failover subscription"));
        }

        if (conf.getTopicsPattern() != null) {
            // If use topicsPattern, we should not use topic(), and topics() method.
            if (!conf.getTopicNames().isEmpty()){
                return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic names list must be null when use topicsPattern"));
            }
            return patternTopicSubscribeAsync(conf, schema, interceptors);
        } else if (conf.getTopicNames().size() == 1) {
            return singleTopicSubscribeAsync(conf, schema, interceptors);
        } else {
            return multiTopicSubscribeAsync(conf, schema, interceptors);
        }
    }

    private <T> CompletableFuture<Consumer<T>> singleTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                         Schema<T> schema,
                                                                         ConsumerInterceptors<T> interceptors) {
        return preProcessSchemaBeforeSubscribe(this, schema, conf.getSingleTopic())
                .thenCompose(schemaClone -> doSingleTopicSubscribeAsync(conf, schemaClone, interceptors));
    }

    private <T> CompletableFuture<Consumer<T>> doSingleTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                           Schema<T> schema,
                                                                           ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        String topic = conf.getSingleTopic();

        getPartitionedTopicMetadata(topic, true, false).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ConsumerBase<T> consumer;
            if (metadata.partitions > 0) {
                consumer = MultiTopicsConsumerImpl.createPartitionedConsumer(PulsarClientImpl.this, conf,
                        externalExecutorProvider, consumerSubscribedFuture, metadata.partitions, schema, interceptors);
            } else {
                int partitionIndex = TopicName.getPartitionIndex(topic);
                consumer = ConsumerImpl.newConsumerImpl(PulsarClientImpl.this, topic, conf, externalExecutorProvider,
                        partitionIndex, false, consumerSubscribedFuture, null, schema, interceptors,
                        true /* createTopicIfDoesNotExist */);
            }
            consumers.add(consumer);
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
            consumerSubscribedFuture.completeExceptionally(ex);
            return null;
        });

        return consumerSubscribedFuture;
    }

    private <T> CompletableFuture<Consumer<T>> multiTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                        Schema<T> schema,
                                                                        ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        ConsumerBase<T> consumer = new MultiTopicsConsumerImpl<>(PulsarClientImpl.this, conf,
                externalExecutorProvider, consumerSubscribedFuture, schema, interceptors,
                true /* createTopicIfDoesNotExist */);

        consumers.add(consumer);

        return consumerSubscribedFuture;
    }

    public CompletableFuture<Consumer<byte[]>> patternTopicSubscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return patternTopicSubscribeAsync(conf, Schema.BYTES, null);
    }

    private <T> CompletableFuture<Consumer<T>> patternTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
            Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        String regex = conf.getTopicsPattern().pattern();
        Mode subscriptionMode = convertRegexSubscriptionMode(conf.getRegexSubscriptionMode());
        TopicName destination = TopicName.get(regex);
        NamespaceName namespaceName = destination.getNamespaceObject();

        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
        lookup.getTopicsUnderNamespace(namespaceName, subscriptionMode, regex, null)
            .thenAccept(getTopicsResult -> {
                if (log.isDebugEnabled()) {
                    log.debug("Pattern consumer [{}] get topics under namespace {}, topics.size: {},"
                                    + " topicsHash: {}, changed: {}, filtered: {}", conf.getSubscriptionName(),
                            namespaceName, getTopicsResult.getTopics().size(), getTopicsResult.getTopicsHash(),
                            getTopicsResult.isChanged(), getTopicsResult.isFiltered());
                    getTopicsResult.getTopics().forEach(topicName ->
                        log.debug("Pattern consumer [{}] get topics under namespace {}, topic: {}",
                                conf.getSubscriptionName(), namespaceName, topicName));
                }

                List<String> topicsList = getTopicsResult.getTopics();
                if (!getTopicsResult.isFiltered()) {
                   topicsList = TopicList.filterTopics(getTopicsResult.getTopics(), conf.getTopicsPattern());
                }
                conf.getTopicNames().addAll(topicsList);

                if (log.isDebugEnabled()) {
                    log.debug("Pattern consumer [{}] initialize topics. {}", conf.getSubscriptionName(),
                            getTopicsResult.getNonPartitionedOrPartitionTopics());
                }

                // Pattern consumer has his unique check mechanism, so do not need the feature "autoUpdatePartitions".
                conf.setAutoUpdatePartitions(false);
                ConsumerBase<T> consumer = new PatternMultiTopicsConsumerImpl<>(conf.getTopicsPattern(),
                        getTopicsResult.getTopicsHash(),
                        PulsarClientImpl.this,
                        conf,
                        externalExecutorProvider,
                        consumerSubscribedFuture,
                        schema, subscriptionMode, interceptors);

                consumers.add(consumer);
            })
            .exceptionally(ex -> {
                log.warn("[{}] Failed to get topics under namespace", namespaceName);
                consumerSubscribedFuture.completeExceptionally(ex);
                return null;
            });

        return consumerSubscribedFuture;
    }

    public CompletableFuture<Reader<byte[]>> createReaderAsync(ReaderConfigurationData<byte[]> conf) {
        return createReaderAsync(conf, Schema.BYTES);
    }

    public <T> CompletableFuture<Reader<T>> createReaderAsync(ReaderConfigurationData<T> conf, Schema<T> schema) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }
        for (String topic : conf.getTopicNames()) {
            if (!TopicName.isValid(topic)) {
                return FutureUtil.failedFuture(new PulsarClientException
                        .InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
            }
        }

        if (conf.getStartMessageId() == null) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Invalid startMessageId"));
        }

        if (conf.getTopicNames().size() == 1) {
            return preProcessSchemaBeforeSubscribe(this, schema, conf.getTopicName())
                    .thenCompose(schemaClone -> createSingleTopicReaderAsync(conf, schemaClone));
        }
        return createMultiTopicReaderAsync(conf, schema);
    }

    protected <T> CompletableFuture<Reader<T>> createMultiTopicReaderAsync(
            ReaderConfigurationData<T> conf, Schema<T> schema) {
        CompletableFuture<Reader<T>> readerFuture = new CompletableFuture<>();
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
        MultiTopicsReaderImpl<T> reader = new MultiTopicsReaderImpl<>(this,
                conf, externalExecutorProvider, consumerSubscribedFuture, schema);
        ConsumerBase<T> consumer = reader.getMultiTopicsConsumer();
        consumers.add(consumer);
        consumerSubscribedFuture.thenRun(() -> readerFuture.complete(reader))
                .exceptionally(ex -> {
                    log.warn("Failed to create multiTopicReader", ex);
                    readerFuture.completeExceptionally(ex);
                    return null;
                });
        return readerFuture;
    }

    protected <T> CompletableFuture<Reader<T>> createSingleTopicReaderAsync(
            ReaderConfigurationData<T> conf, Schema<T> schema) {
        String topic = conf.getTopicName();

        CompletableFuture<Reader<T>> readerFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic, true, false).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }
            if (metadata.partitions > 0
                    && MultiTopicsConsumerImpl.isIllegalMultiTopicsMessageId(conf.getStartMessageId())) {
                readerFuture.completeExceptionally(
                        new PulsarClientException("The partitioned topic startMessageId is illegal"));
                return;
            }
            CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
            Reader<T> reader;
            ConsumerBase<T> consumer;
            if (metadata.partitions > 0) {
                reader = new MultiTopicsReaderImpl<>(PulsarClientImpl.this,
                        conf, externalExecutorProvider, consumerSubscribedFuture, schema);
                consumer = ((MultiTopicsReaderImpl<T>) reader).getMultiTopicsConsumer();
            } else {
                reader = new ReaderImpl<>(PulsarClientImpl.this, conf, externalExecutorProvider,
                        consumerSubscribedFuture, schema);
                consumer = ((ReaderImpl<T>) reader).getConsumer();
            }

            consumers.add(consumer);

            consumerSubscribedFuture.thenRun(() -> readerFuture.complete(reader)).exceptionally(ex -> {
                log.warn("[{}] Failed to get create topic reader", topic, ex);
                readerFuture.completeExceptionally(ex);
                return null;
            });
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
            readerFuture.completeExceptionally(ex);
            return null;
        });

        return readerFuture;
    }

    /**
     * Read the schema information for a given topic.
     *
     * If the topic does not exist or it has no schema associated, it will return an empty response
     */
    public CompletableFuture<Optional<SchemaInfo>> getSchema(String topic) {
        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
        } catch (Throwable t) {
            return FutureUtil
                    .failedFuture(
                            new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }

        return lookup.getSchema(topicName);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            PulsarClientException unwrapped = PulsarClientException.unwrap(e);
            if (unwrapped instanceof PulsarClientException.AlreadyClosedException) {
                // this is not a problem
                return;
            }
            throw unwrapped;
        }
    }

    private void closeUrlLookupMap() {
        Map<String, LookupService> closedUrlLookupServices = new HashMap(urlLookupMap.size());
        urlLookupMap.entrySet().forEach(e -> {
            try {
                e.getValue().close();
            } catch (Exception ex) {
                log.error("Error closing lookup service {}", e.getKey(), ex);
            }
            closedUrlLookupServices.put(e.getKey(), e.getValue());
        });
        closedUrlLookupServices.entrySet().forEach(e -> {
            urlLookupMap.remove(e.getKey(), e.getValue());
        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        log.info("Client closing. URL: {}", lookup.getServiceUrl());
        if (!state.compareAndSet(State.Open, State.Closing)) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        closeUrlLookupMap();

        producers.forEach(p -> futures.add(p.closeAsync().handle((__, t) -> {
            if (t != null) {
                log.error("Error closing producer {}", p, t);
            }
            return null;
        })));
        consumers.forEach(c -> futures.add(c.closeAsync().handle((__, t) -> {
            if (t != null) {
                log.error("Error closing consumer {}", c, t);
            }
            return null;
        })));

        // Need to run the shutdown sequence in a separate thread to prevent deadlocks
        // If there are consumers or producers that need to be shutdown we cannot use the same thread
        // to shutdown the EventLoopGroup as well as that would be trying to shutdown itself thus a deadlock
        // would happen
        CompletableFuture<Void> combinedFuture = FutureUtil.waitForAll(futures);
        ScheduledExecutorService shutdownExecutor = Executors.newSingleThreadScheduledExecutor(
                new ExecutorProvider.ExtendedThreadFactory("pulsar-client-shutdown-timeout-scheduler"));
        FutureUtil.addTimeoutHandling(combinedFuture, Duration.ofSeconds(CLOSE_TIMEOUT_SECONDS),
                shutdownExecutor, () -> FutureUtil.createTimeoutException("Closing producers and consumers timed out.",
                        PulsarClientImpl.class, "closeAsync"));
        combinedFuture.handle((__, t) -> {
            if (t != null) {
                log.error("Closing producers and consumers failed. Continuing with shutdown.", t);
            }
            new Thread(() -> {
                shutdownExecutor.shutdownNow();
                // All producers & consumers are now closed, we can stop the client safely
                try {
                    shutdown();
                } catch (PulsarClientException e) {
                    log.error("Shutdown failed. Ignoring the exception.", e);
                }
                state.set(State.Closed);
                closeFuture.complete(null);
            }, "pulsar-client-shutdown-thread").start();
            return null;
        });
        return closeFuture;
    }

    @Override
    public void shutdown() throws PulsarClientException {
        try {
            // We will throw the last thrown exception only, though logging all of them.
            Throwable throwable = null;
            if (lookup != null) {
                try {
                    lookup.close();
                } catch (Throwable t) {
                    log.warn("Failed to shutdown lookup", t);
                    throwable = t;
                }
            }
            if (tcClient != null) {
                try {
                    tcClient.close();
                } catch (Throwable t) {
                    log.warn("Failed to close tcClient");
                    throwable = t;
                }
            }

            // close the service url provider allocated resource.
            if (conf != null && conf.getServiceUrlProvider() != null) {
                conf.getServiceUrlProvider().close();
            }

            try {
                // Shutting down eventLoopGroup separately because in some cases, cnxPool might be using different
                // eventLoopGroup.
                shutdownEventLoopGroup(eventLoopGroup);
            } catch (PulsarClientException e) {
                log.warn("Failed to shutdown eventLoopGroup", e);
                throwable = e;
            }
            try {
                closeCnxPool(cnxPool);
            } catch (PulsarClientException e) {
                log.warn("Failed to shutdown cnxPool", e);
                throwable = e;
            }
            if (timer != null && needStopTimer) {
                try {
                    timer.stop();
                } catch (Throwable t) {
                    log.warn("Failed to shutdown timer", t);
                    throwable = t;
                }
            }
            try {
                shutdownExecutors();
            } catch (PulsarClientException e) {
                throwable = e;
            }
            if (conf != null && conf.getAuthentication() != null) {
                try {
                    conf.getAuthentication().close();
                } catch (Throwable t) {
                    log.warn("Failed to close authentication", t);
                    throwable = t;
                }
            }
            if (throwable != null) {
                throw throwable;
            }
        } catch (Throwable t) {
            log.warn("Failed to shutdown Pulsar client", t);
            throw PulsarClientException.unwrap(t);
        }
    }

    private void closeCnxPool(ConnectionPool cnxPool) throws PulsarClientException {
        if (createdCnxPool && cnxPool != null) {
            try {
                cnxPool.close();
            } catch (Throwable t) {
                throw PulsarClientException.unwrap(t);
            }
        }
    }

    private void shutdownEventLoopGroup(EventLoopGroup eventLoopGroup) throws PulsarClientException {
        if (createdEventLoopGroup && eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
            try {
                eventLoopGroup.shutdownGracefully().get(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Throwable t) {
                throw PulsarClientException.unwrap(t);
            }
        }
    }

    private void shutdownExecutors() throws PulsarClientException {
        PulsarClientException pulsarClientException = null;
        if (createdExecutorProviders) {

            if (externalExecutorProvider != null && !externalExecutorProvider.isShutdown()) {
                try {
                    externalExecutorProvider.shutdownNow();
                } catch (Throwable t) {
                    log.warn("Failed to shutdown externalExecutorProvider", t);
                    pulsarClientException = PulsarClientException.unwrap(t);
                }
            }
            if (internalExecutorProvider != null && !internalExecutorProvider.isShutdown()) {
                try {
                    internalExecutorProvider.shutdownNow();
                } catch (Throwable t) {
                    log.warn("Failed to shutdown internalExecutorService", t);
                    pulsarClientException = PulsarClientException.unwrap(t);
                }
            }
        }
        if (createdScheduledProviders && scheduledExecutorProvider != null && !scheduledExecutorProvider.isShutdown()) {
            try {
                scheduledExecutorProvider.shutdownNow();
            } catch (Throwable t) {
                log.warn("Failed to shutdown scheduledExecutorProvider", t);
                pulsarClientException = PulsarClientException.unwrap(t);
            }
        }

        if (createdLookupProviders && lookupExecutorProvider != null && !lookupExecutorProvider.isShutdown()) {
            try {
                lookupExecutorProvider.shutdownNow();
            } catch (Throwable t) {
                log.warn("Failed to shutdown lookupExecutorProvider", t);
                pulsarClientException = PulsarClientException.unwrap(t);
            }
        }

        if (pulsarClientException != null) {
            throw pulsarClientException;
        }
    }

    @Override
    public boolean isClosed() {
        State currentState = state.get();
        return currentState == State.Closed || currentState == State.Closing;
    }

    @Override
    public synchronized void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        log.info("Updating service URL to {}", serviceUrl);

        conf.setServiceUrl(serviceUrl);
        lookup.updateServiceUrl(serviceUrl);
        cnxPool.closeAllConnections();
    }

    public void updateAuthentication(Authentication authentication) throws IOException {
        log.info("Updating authentication to {}", authentication);
        if (conf.getAuthentication() != null) {
            conf.getAuthentication().close();
        }
        conf.setAuthentication(authentication);
        conf.getAuthentication().start();
    }

    public void updateTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        log.info("Updating tlsTrustCertsFilePath to {}", tlsTrustCertsFilePath);
        conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
    }

    public void updateTlsTrustStorePathAndPassword(String tlsTrustStorePath, String tlsTrustStorePassword) {
        log.info("Updating tlsTrustStorePath to {}, tlsTrustStorePassword to *****", tlsTrustStorePath);
        conf.setTlsTrustStorePath(tlsTrustStorePath);
        conf.setTlsTrustStorePassword(tlsTrustStorePassword);
    }

    public CompletableFuture<ClientCnx> getConnection(final String topic, int randomKeyForSelectConnection) {
        TopicName topicName = TopicName.get(topic);
        return lookup.getBroker(topicName)
                .thenCompose(pair -> getConnection(pair.getLeft(), pair.getRight(), randomKeyForSelectConnection));
    }

    /**
     * Only for test.
     */
    @VisibleForTesting
    public CompletableFuture<ClientCnx> getConnection(final String topic) {
        TopicName topicName = TopicName.get(topic);
        return lookup.getBroker(topicName)
                .thenCompose(pair -> getConnection(pair.getLeft(), pair.getRight(), cnxPool.genRandomKeyToSelectCon()));
    }

    public CompletableFuture<ClientCnx> getConnection(final String topic, final String url) {
        TopicName topicName = TopicName.get(topic);
        return getLookup(url).getBroker(topicName)
                .thenCompose(pair -> getConnection(pair.getLeft(), pair.getRight(), cnxPool.genRandomKeyToSelectCon()));
    }

    public LookupService getLookup(String serviceUrl) {
        return urlLookupMap.computeIfAbsent(serviceUrl, url -> {
            if (isClosed()) {
                throw new IllegalStateException("Pulsar client has been closed, can not build LookupService when"
                        + " calling get lookup with an url");
            }
            try {
                return createLookup(serviceUrl);
            } catch (PulsarClientException e) {
                log.warn("Failed to update url to lookup service {}, {}", url, e.getMessage());
                throw new IllegalStateException("Failed to update url " + url);
            }
        });
    }

    public CompletableFuture<ClientCnx> getConnectionToServiceUrl() {
        if (!(lookup instanceof BinaryProtoLookupService)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidServiceURL(
                    "Can't get client connection to HTTP service URL", null));
        }
        InetSocketAddress address = lookup.resolveHost();
        return getConnection(address, address, cnxPool.genRandomKeyToSelectCon());
    }

    public CompletableFuture<ClientCnx> getConnection(final InetSocketAddress logicalAddress,
                                                      final InetSocketAddress physicalAddress,
                                                      final int randomKeyForSelectConnection) {
        return cnxPool.getConnection(logicalAddress, physicalAddress, randomKeyForSelectConnection);
    }

    /** visible for pulsar-functions. **/
    public Timer timer() {
        return timer;
    }

    public ExecutorProvider externalExecutorProvider() {
        return externalExecutorProvider;
    }

    long newProducerId() {
        return producerIdGenerator.getAndIncrement();
    }

    long newConsumerId() {
        return consumerIdGenerator.getAndIncrement();
    }

    long newTopicListWatcherId() {
        return topicListWatcherIdGenerator.getAndIncrement();
    }

    public long newRequestId() {
        return requestIdGenerator.getAndIncrement();
    }

    public ConnectionPool getCnxPool() {
        return cnxPool;
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    @VisibleForTesting
    public void setLookup(LookupService lookup) {
        this.lookup = lookup;
    }

    public LookupService getLookup() {
        return lookup;
    }

    public void reloadLookUp() throws PulsarClientException {
        lookup = createLookup(conf.getServiceUrl());
    }

    public LookupService createLookup(String url) throws PulsarClientException {
        if (url.startsWith("http")) {
            return new HttpLookupService(conf, eventLoopGroup);
        } else {
            return new BinaryProtoLookupService(this, url, conf.getListenerName(), conf.isUseTls(),
                    externalExecutorProvider.getExecutor());
        }
    }

    /**
     * @param useFallbackForNonPIP344Brokers <p>If true, fallback to the prior behavior of the method
     *                                       getPartitionedTopicMetadata if the broker does not support the PIP-344
     *                                       feature 'supports_get_partitioned_metadata_without_auto_creation'. This
     *                                       parameter only affects the behavior when
     *                                       {@param metadataAutoCreationEnabled} is false.</p>
     */
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(
            String topic, boolean metadataAutoCreationEnabled, boolean useFallbackForNonPIP344Brokers) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();

        try {
            TopicName topicName = TopicName.get(topic);
            AtomicLong opTimeoutMs = new AtomicLong(conf.getLookupTimeoutMs());
            Backoff backoff = new BackoffBuilder()
                    .setInitialTime(conf.getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                    .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                    .setMax(conf.getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                    .create();
            getPartitionedTopicMetadata(topicName, backoff, opTimeoutMs, metadataFuture, new ArrayList<>(),
                    metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
        } catch (IllegalArgumentException e) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(e.getMessage()));
        }
        return metadataFuture;
    }

    private void getPartitionedTopicMetadata(TopicName topicName,
                                             Backoff backoff,
                                             AtomicLong remainingTime,
                                             CompletableFuture<PartitionedTopicMetadata> future,
                                             List<Throwable> previousExceptions,
                                             boolean metadataAutoCreationEnabled,
                                             boolean useFallbackForNonPIP344Brokers) {
        long startTime = System.nanoTime();
        CompletableFuture<PartitionedTopicMetadata> queryFuture = lookup.getPartitionedTopicMetadata(topicName,
                metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
        queryFuture.thenAccept(future::complete).exceptionally(e -> {
            remainingTime.addAndGet(-1 * TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            // skip retry scheduler when set lookup throttle in client or server side which will lead to
            // `TooManyRequestsException`
            boolean isLookupThrottling = !PulsarClientException.isRetriableError(e.getCause())
                || e.getCause() instanceof PulsarClientException.AuthenticationException
                || e.getCause() instanceof PulsarClientException.NotFoundException;
            if (nextDelay <= 0 || isLookupThrottling) {
                PulsarClientException.setPreviousExceptions(e, previousExceptions);
                future.completeExceptionally(e);
                return null;
            }
            previousExceptions.add(e);

            ((ScheduledExecutorService) scheduledExecutorProvider.getExecutor()).schedule(() -> {
                log.warn("[topic: {}] Could not get connection while getPartitionedTopicMetadata -- "
                        + "Will try again in {} ms", topicName, nextDelay);
                remainingTime.addAndGet(-nextDelay);
                getPartitionedTopicMetadata(topicName, backoff, remainingTime, future, previousExceptions,
                        metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
            }, nextDelay, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @Override
    public CompletableFuture<List<String>> getPartitionsForTopic(String topic, boolean metadataAutoCreationEnabled) {
        return getPartitionedTopicMetadata(topic, metadataAutoCreationEnabled, false).thenApply(metadata -> {
            if (metadata.partitions > 0) {
                TopicName topicName = TopicName.get(topic);
                List<String> partitions = new ArrayList<>(metadata.partitions);
                for (int i = 0; i < metadata.partitions; i++) {
                    partitions.add(topicName.getPartition(i).toString());
                }
                return partitions;
            } else {
                return Collections.singletonList(topic);
            }
        });
    }

    private static EventLoopGroup getEventLoopGroup(ClientConfigurationData conf) {
        ThreadFactory threadFactory = getThreadFactory("pulsar-client-io");
        return EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), conf.isEnableBusyWait(), threadFactory);
    }

    private static ThreadFactory getThreadFactory(String poolName) {
        return new ExecutorProvider.ExtendedThreadFactory(poolName, Thread.currentThread().isDaemon());
    }

    void cleanupProducer(ProducerBase<?> producer) {
        producers.remove(producer);
    }

    void cleanupConsumer(ConsumerBase<?> consumer) {
        consumers.remove(consumer);
    }

    @VisibleForTesting
    int producersCount() {
        return producers.size();
    }

    @VisibleForTesting
    int consumersCount() {
        return consumers.size();
    }

    private static Mode convertRegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode) {
        switch (regexSubscriptionMode) {
        case PersistentOnly:
            return Mode.PERSISTENT;
        case NonPersistentOnly:
            return Mode.NON_PERSISTENT;
        case AllTopics:
            return Mode.ALL;
        default:
            return null;
        }
    }

    private SchemaInfoProvider newSchemaProvider(String topicName) {
        return new MultiVersionSchemaInfoProvider(TopicName.get(topicName), this);
    }

    public LoadingCache<String, SchemaInfoProvider> getSchemaProviderLoadingCache() {
        return schemaProviderLoadingCache;
    }

    public MemoryLimitController getMemoryLimitController() {
        return memoryLimitController;
    }

    @SuppressWarnings("unchecked")
    protected <T> CompletableFuture<Schema<T>> preProcessSchemaBeforeSubscribe(PulsarClientImpl pulsarClientImpl,
                                                                      Schema<T> schema,
                                                                      String topicName) {
        if (schema != null && schema.supportSchemaVersioning()) {
            final SchemaInfoProvider schemaInfoProvider;
            try {
                schemaInfoProvider = pulsarClientImpl.getSchemaProviderLoadingCache().get(topicName);
            } catch (ExecutionException e) {
                log.error("Failed to load schema info provider for topic {}", topicName, e);
                return FutureUtil.failedFuture(e.getCause());
            }
            schema = schema.clone();
            if (schema.requireFetchingSchemaInfo()) {
                @SuppressWarnings("rawtypes") Schema finalSchema = schema;
                return schemaInfoProvider.getLatestSchema().thenCompose(schemaInfo -> {
                    if (null == schemaInfo) {
                        if (!(finalSchema instanceof AutoConsumeSchema)
                            && !(finalSchema instanceof KeyValueSchema)) {
                            // no schema info is found
                            return FutureUtil.failedFuture(
                                    new PulsarClientException.NotFoundException(
                                            "No latest schema found for topic " + topicName));
                        }
                    }
                    try {
                        log.info("Configuring schema for topic {} : {}", topicName, schemaInfo);
                        finalSchema.configureSchemaInfo(topicName, "topic", schemaInfo);
                    } catch (RuntimeException re) {
                        return FutureUtil.failedFuture(re);
                    }
                    finalSchema.setSchemaInfoProvider(schemaInfoProvider);
                    return CompletableFuture.completedFuture(finalSchema);
                });
            } else {
                schema.setSchemaInfoProvider(schemaInfoProvider);
            }
        }
        return CompletableFuture.completedFuture(schema);
    }

    public ExecutorService getInternalExecutorService() {
        return internalExecutorProvider.getExecutor();
    }

    public ScheduledExecutorProvider getScheduledExecutorProvider() {
        return scheduledExecutorProvider;
    }

    //
    // Transaction related API
    //

    // This method should be exposed in the PulsarClient interface. Only expose it when all the transaction features
    // are completed.
    // @Override
    public TransactionBuilder newTransaction() {
        return new TransactionBuilderImpl(this, tcClient);
    }
}
