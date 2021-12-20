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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
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
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.client.impl.transaction.TransactionBuilderImpl;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarClientImpl implements PulsarClient {

    private static final Logger log = LoggerFactory.getLogger(PulsarClientImpl.class);

    protected final ClientConfigurationData conf;
    private LookupService lookup;
    private final ConnectionPool cnxPool;
    @Getter
    private final Timer timer;
    private boolean needStopTimer;
    private final ExecutorProvider externalExecutorProvider;
    private final ExecutorProvider internalExecutorService;
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
    private final AtomicLong requestIdGenerator
        = new AtomicLong(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE/2));

    protected final EventLoopGroup eventLoopGroup;
    private final MemoryLimitController memoryLimitController;

    private final LoadingCache<String, SchemaInfoProvider> schemaProviderLoadingCache = CacheBuilder.newBuilder().maximumSize(100000)
                    .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<String, SchemaInfoProvider>() {

                @Override
                public SchemaInfoProvider load(String topicName) {
                    return newSchemaProvider(topicName);
                }
            });

    private final Clock clientClock;

    @Getter
    private TransactionCoordinatorClientImpl tcClient;

    public PulsarClientImpl(ClientConfigurationData conf) throws PulsarClientException {
        this(conf, getEventLoopGroup(conf), true);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, new ConnectionPool(conf, eventLoopGroup), null, false, true);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, null, false, false);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool, Timer timer)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, timer, false, false);
    }

    private PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, boolean createdEventLoopGroup)
            throws PulsarClientException {
        this(conf, eventLoopGroup, new ConnectionPool(conf, eventLoopGroup), null, createdEventLoopGroup, true);
    }

    private PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool, Timer timer,
                             boolean createdEventLoopGroup, boolean createdCnxPool) throws PulsarClientException {
        try {
            this.createdEventLoopGroup = createdEventLoopGroup;
            this.createdCnxPool = createdCnxPool;
            if (conf == null || isBlank(conf.getServiceUrl()) || eventLoopGroup == null) {
                throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
            }
            this.eventLoopGroup = eventLoopGroup;
            setAuth(conf);
            this.conf = conf;
            clientClock = conf.getClock();
            conf.getAuthentication().start();
            this.cnxPool = cnxPool;
            externalExecutorProvider = new ExecutorProvider(conf.getNumListenerThreads(), "pulsar-external-listener");
            internalExecutorService = new ExecutorProvider(conf.getNumIoThreads(), "pulsar-client-internal");
            if (conf.getServiceUrl().startsWith("http")) {
                lookup = new HttpLookupService(conf, eventLoopGroup);
            } else {
                lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.getListenerName(), conf.isUseTls(), externalExecutorProvider.getExecutor());
            }
            if (timer == null) {
                this.timer = new HashedWheelTimer(getThreadFactory("pulsar-timer"), 1, TimeUnit.MILLISECONDS);
                needStopTimer = true;
            } else {
                this.timer = timer;
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

            memoryLimitController = new MemoryLimitController(conf.getMemoryLimitBytes());
            state.set(State.Open);
        } catch (Throwable t) {
            shutdown();
            shutdownEventLoopGroup(eventLoopGroup);
            closeCnxPool(cnxPool);
            throw t;
        }
    }

    private void setAuth(ClientConfigurationData conf) throws PulsarClientException {
        if (StringUtils.isBlank(conf.getAuthPluginClassName())
                || (StringUtils.isBlank(conf.getAuthParams()) && conf.getAuthParamMap() == null)) {
            return;
        }

        if (StringUtils.isNotBlank(conf.getAuthParams())) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParams()));
        } else if (conf.getAuthParamMap() != null) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParamMap()));
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
        return new ProducerBuilderImpl<>(this, schema);
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
                new PulsarClientException.InvalidConfigurationException("AutoConsumeSchema is only used by consumers to detect schemas automatically"));
        }

        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed : state = " + state.get()));
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
                            autoProduceBytesSchema.setSchema(Schema.getSchema(schemaInfoOptional.get()));
                        } else {
                            autoProduceBytesSchema.setSchema(Schema.BYTES);
                        }
                        return createProducerAsync(topic, conf, schema, interceptors);
                    });
        } else {
            return createProducerAsync(topic, conf, schema, interceptors);
        }

    }

    private <T> CompletableFuture<Producer<T>> createProducerAsync(String topic,
                                                                   ProducerConfigurationData conf,
                                                                   Schema<T> schema,
                                                                   ProducerInterceptors interceptors) {
        CompletableFuture<Producer<T>> producerCreatedFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ProducerBase<T> producer;
            if (metadata.partitions > 0) {
                producer = newPartitionedProducerImpl(topic, conf, schema, interceptors, producerCreatedFuture,
                        metadata);
            } else {
                producer = newProducerImpl(topic, -1, conf, schema, interceptors, producerCreatedFuture);
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
     * @param metadata partitioned topic metadata
     * @param <T> message type class
     * @return new PartitionedProducerImpl instance
     */
    protected <T> PartitionedProducerImpl<T> newPartitionedProducerImpl(String topic,
                                                                        ProducerConfigurationData conf,
                                                                        Schema<T> schema,
                                                                        ProducerInterceptors interceptors,
                                                                        CompletableFuture<Producer<T>> producerCreatedFuture,
                                                                        PartitionedTopicMetadata metadata) {
        return new PartitionedProducerImpl<>(PulsarClientImpl.this, topic, conf, metadata.partitions,
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
                                                  CompletableFuture<Producer<T>> producerCreatedFuture) {
        return new ProducerImpl<>(PulsarClientImpl.this, topic, conf, producerCreatedFuture, partitionIndex, schema,
                interceptors);
    }

    public CompletableFuture<Consumer<byte[]>> subscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return subscribeAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Consumer<T>> subscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        for (String topic : conf.getTopicNames()) {
            if (!TopicName.isValid(topic)) {
                return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
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

    private <T> CompletableFuture<Consumer<T>> singleTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        return preProcessSchemaBeforeSubscribe(this, schema, conf.getSingleTopic())
            .thenCompose(schemaClone -> doSingleTopicSubscribeAsync(conf, schemaClone, interceptors));
    }

    private <T> CompletableFuture<Consumer<T>> doSingleTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        String topic = conf.getSingleTopic();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
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

    private <T> CompletableFuture<Consumer<T>> multiTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
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
        lookup.getTopicsUnderNamespace(namespaceName, subscriptionMode)
            .thenAccept(topics -> {
                if (log.isDebugEnabled()) {
                    log.debug("Get topics under namespace {}, topics.size: {}", namespaceName.toString(), topics.size());
                    topics.forEach(topicName ->
                        log.debug("Get topics under namespace {}, topic: {}", namespaceName.toString(), topicName));
                }

                List<String> topicsList = topicsPatternFilter(topics, conf.getTopicsPattern());
                conf.getTopicNames().addAll(topicsList);
                ConsumerBase<T> consumer = new PatternMultiTopicsConsumerImpl<>(conf.getTopicsPattern(),
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

    // get topics that match 'topicsPattern' from original topics list
    // return result should contain only topic names, without partition part
    public static List<String> topicsPatternFilter(List<String> original, Pattern topicsPattern) {
        final Pattern shortenedTopicsPattern = topicsPattern.toString().contains("://")
            ? Pattern.compile(topicsPattern.toString().split("\\:\\/\\/")[1]) : topicsPattern;

        return original.stream()
            .map(TopicName::get)
            .map(TopicName::toString)
            .filter(topic -> shortenedTopicsPattern.matcher(topic.split("\\:\\/\\/")[1]).matches())
            .collect(Collectors.toList());
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

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }
            if (metadata.partitions > 0 && MultiTopicsConsumerImpl.isIllegalMultiTopicsMessageId(conf.getStartMessageId())) {
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
                reader = new ReaderImpl<>(PulsarClientImpl.this, conf, externalExecutorProvider, consumerSubscribedFuture, schema);
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
                    .failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
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

    @Override
    public CompletableFuture<Void> closeAsync() {
        log.info("Client closing. URL: {}", lookup.getServiceUrl());
        if (!state.compareAndSet(State.Open, State.Closing)) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        producers.forEach(p -> futures.add(p.closeAsync()));
        consumers.forEach(c -> futures.add(c.closeAsync()));

        // Need to run the shutdown sequence in a separate thread to prevent deadlocks
        // If there are consumers or producers that need to be shutdown we cannot use the same thread
        // to shutdown the EventLoopGroup as well as that would be trying to shutdown itself thus a deadlock
        // would happen
        FutureUtil.waitForAll(futures).thenRun(() -> new Thread(() -> {
            // All producers & consumers are now closed, we can stop the client safely
            try {
                shutdown();
                closeFuture.complete(null);
                state.set(State.Closed);
            } catch (PulsarClientException e) {
                closeFuture.completeExceptionally(e);
            }
        }, "pulsar-client-shutdown-thread").start()).exceptionally(exception -> {
            closeFuture.completeExceptionally(exception);
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
                eventLoopGroup.shutdownGracefully().get();
            } catch (Throwable t) {
                throw PulsarClientException.unwrap(t);
            }
        }
    }

    private void shutdownExecutors() throws PulsarClientException {
        PulsarClientException pulsarClientException = null;

        if (externalExecutorProvider != null && !externalExecutorProvider.isShutdown()) {
            try {
                externalExecutorProvider.shutdownNow();
            } catch (Throwable t) {
                log.warn("Failed to shutdown externalExecutorProvider", t);
                pulsarClientException = PulsarClientException.unwrap(t);
            }
        }
        if (internalExecutorService != null && !internalExecutorService.isShutdown()) {
            try {
                internalExecutorService.shutdownNow();
            } catch (Throwable t) {
                log.warn("Failed to shutdown internalExecutorService", t);
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

    public CompletableFuture<ClientCnx> getConnection(final String topic) {
        TopicName topicName = TopicName.get(topic);
        return lookup.getBroker(topicName)
                .thenCompose(pair -> cnxPool.getConnection(pair.getLeft(), pair.getRight()));
    }

    /** visible for pulsar-functions **/
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

    public long newRequestId() {
        return requestIdGenerator.getAndIncrement();
    }

    public ConnectionPool getCnxPool() {
        return cnxPool;
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    public LookupService getLookup() {
        return lookup;
    }

    public void reloadLookUp() throws PulsarClientException {
        if (conf.getServiceUrl().startsWith("http")) {
            lookup = new HttpLookupService(conf, eventLoopGroup);
        } else {
            lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.getListenerName(), conf.isUseTls(),
                    externalExecutorProvider.getExecutor());
        }
    }

    public CompletableFuture<Integer> getNumberOfPartitions(String topic) {
        return getPartitionedTopicMetadata(topic).thenApply(metadata -> metadata.partitions);
    }

    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(String topic) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();

        try {
            TopicName topicName = TopicName.get(topic);
            AtomicLong opTimeoutMs = new AtomicLong(conf.getLookupTimeoutMs());
            Backoff backoff = new BackoffBuilder()
                    .setInitialTime(100, TimeUnit.MILLISECONDS)
                    .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                    .setMax(1, TimeUnit.MINUTES)
                    .create();
            getPartitionedTopicMetadata(topicName, backoff, opTimeoutMs,
                                        metadataFuture, new ArrayList<>());
        } catch (IllegalArgumentException e) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(e.getMessage()));
        }
        return metadataFuture;
    }

    private void getPartitionedTopicMetadata(TopicName topicName,
                                             Backoff backoff,
                                             AtomicLong remainingTime,
                                             CompletableFuture<PartitionedTopicMetadata> future,
                                             List<Throwable> previousExceptions) {
        long startTime = System.nanoTime();
        lookup.getPartitionedTopicMetadata(topicName).thenAccept(future::complete).exceptionally(e -> {
            remainingTime.addAndGet(-1 * TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            // skip retry scheduler when set lookup throttle in client or server side which will lead to `TooManyRequestsException`
            boolean isLookupThrottling = !PulsarClientException.isRetriableError(e.getCause())
                || e.getCause() instanceof PulsarClientException.AuthenticationException;
            if (nextDelay <= 0 || isLookupThrottling) {
                PulsarClientException.setPreviousExceptions(e, previousExceptions);
                future.completeExceptionally(e);
                return null;
            }
            previousExceptions.add(e);

            ((ScheduledExecutorService) externalExecutorProvider.getExecutor()).schedule(() -> {
                log.warn("[topic: {}] Could not get connection while getPartitionedTopicMetadata -- Will try again in {} ms",
                    topicName, nextDelay);
                remainingTime.addAndGet(-nextDelay);
                getPartitionedTopicMetadata(topicName, backoff, remainingTime, future, previousExceptions);
            }, nextDelay, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @Override
    public CompletableFuture<List<String>> getPartitionsForTopic(String topic) {
        return getPartitionedTopicMetadata(topic).thenApply(metadata -> {
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
        return new DefaultThreadFactory(poolName, Thread.currentThread().isDaemon());
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
        return internalExecutorService.getExecutor();
    }
    //
    // Transaction related API
    //

    // This method should be exposed in the PulsarClient interface. Only expose it when all the transaction features
    // are completed.
    // @Override
    public TransactionBuilder newTransaction() throws PulsarClientException {
        if (!conf.isEnableTransaction()) {
            throw new PulsarClientException.InvalidConfigurationException("Transactions are not enabled");
        }
        return new TransactionBuilderImpl(this, tcClient);
    }

}
