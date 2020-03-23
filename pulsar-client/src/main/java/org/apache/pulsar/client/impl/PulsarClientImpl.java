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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.client.impl.transaction.TransactionBuilderImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace.Mode;
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

    private final ClientConfigurationData conf;
    private LookupService lookup;
    private final ConnectionPool cnxPool;
    private final Timer timer;
    private final ExecutorProvider externalExecutorProvider;

    public enum State {
        Open, Closing, Closed
    }

    private AtomicReference<State> state = new AtomicReference<>();
    private final IdentityHashMap<ProducerBase<?>, Boolean> producers;
    private final IdentityHashMap<ConsumerBase<?>, Boolean> consumers;

    private final AtomicLong producerIdGenerator = new AtomicLong();
    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong requestIdGenerator = new AtomicLong();

    private final EventLoopGroup eventLoopGroup;

    private final LoadingCache<String, SchemaInfoProvider> schemaProviderLoadingCache = CacheBuilder.newBuilder().maximumSize(100000)
                    .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<String, SchemaInfoProvider>() {

                @Override
                public SchemaInfoProvider load(String topicName) {
                    return newSchemaProvider(topicName);
                }
            });

    private final Clock clientClock;

    public PulsarClientImpl(ClientConfigurationData conf) throws PulsarClientException {
        this(conf, getEventLoopGroup(conf));
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, new ConnectionPool(conf, eventLoopGroup));
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool)
            throws PulsarClientException {
        if (conf == null || isBlank(conf.getServiceUrl()) || eventLoopGroup == null) {
            throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
        }
        this.eventLoopGroup = eventLoopGroup;
        setAuth(conf);
        this.conf = conf;
        this.clientClock = conf.getClock();
        conf.getAuthentication().start();
        this.cnxPool = cnxPool;
        externalExecutorProvider = new ExecutorProvider(conf.getNumListenerThreads(), getThreadFactory("pulsar-external-listener"));
        if (conf.getServiceUrl().startsWith("http")) {
            lookup = new HttpLookupService(conf, eventLoopGroup);
        } else {
            lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.isUseTls(), externalExecutorProvider.getExecutor());
        }
        timer = new HashedWheelTimer(getThreadFactory("pulsar-timer"), 1, TimeUnit.MILLISECONDS);
        producers = Maps.newIdentityHashMap();
        consumers = Maps.newIdentityHashMap();
        state.set(State.Open);
    }

    private void setAuth(ClientConfigurationData conf) throws PulsarClientException {
        if (StringUtils.isBlank(conf.getAuthPluginClassName()) || StringUtils.isBlank( conf.getAuthParams())) {
            return;
        }

        conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParams()));
    }

    public ClientConfigurationData getConfiguration() {
        return conf;
    }

    @VisibleForTesting
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
                producer = new PartitionedProducerImpl<>(PulsarClientImpl.this, topic, conf, metadata.partitions,
                        producerCreatedFuture, schema, interceptors);
            } else {
                producer = new ProducerImpl<>(PulsarClientImpl.this, topic, conf, producerCreatedFuture, -1, schema, interceptors);
            }

            synchronized (producers) {
                producers.put(producer, Boolean.TRUE);
            }
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata: {}", topic, ex.getMessage());
            producerCreatedFuture.completeExceptionally(ex);
            return null;
        });

        return producerCreatedFuture;
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

        if (!conf.getTopicNames().stream().allMatch(TopicName::isValid)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
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
                    "Read compacted can only be used with exclusive of failover persistent subscriptions"));
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
            // gets the next single threaded executor from the list of executors
            ExecutorService listenerThread = externalExecutorProvider.getExecutor();
            if (metadata.partitions > 0) {
                consumer = MultiTopicsConsumerImpl.createPartitionedConsumer(PulsarClientImpl.this, conf,
                    listenerThread, consumerSubscribedFuture, metadata.partitions, schema, interceptors);
            } else {
                int partitionIndex = TopicName.getPartitionIndex(topic);
                consumer = ConsumerImpl.newConsumerImpl(PulsarClientImpl.this, topic, conf, listenerThread, partitionIndex, false,
                        consumerSubscribedFuture,null, schema, interceptors,
                        true /* createTopicIfDoesNotExist */);
            }

            synchronized (consumers) {
                consumers.put(consumer, Boolean.TRUE);
            }
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
                externalExecutorProvider.getExecutor(), consumerSubscribedFuture, schema, interceptors,
                true /* createTopicIfDoesNotExist */);

        synchronized (consumers) {
            consumers.put(consumer, Boolean.TRUE);
        }

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
                ConsumerBase<T> consumer = new PatternMultiTopicsConsumerImpl<T>(conf.getTopicsPattern(),
                    PulsarClientImpl.this,
                    conf,
                    externalExecutorProvider.getExecutor(),
                    consumerSubscribedFuture,
                    schema, subscriptionMode, interceptors);

                synchronized (consumers) {
                    consumers.put(consumer, Boolean.TRUE);
                }
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
        return preProcessSchemaBeforeSubscribe(this, schema, conf.getTopicName())
            .thenCompose(schemaClone -> doCreateReaderAsync(conf, schemaClone));
    }

    <T> CompletableFuture<Reader<T>> doCreateReaderAsync(ReaderConfigurationData<T> conf, Schema<T> schema) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        String topic = conf.getTopicName();

        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
        }

        if (conf.getStartMessageId() == null) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Invalid startMessageId"));
        }

        CompletableFuture<Reader<T>> readerFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            if (metadata.partitions > 0) {
                readerFuture.completeExceptionally(
                        new PulsarClientException("Topic reader cannot be created on a partitioned topic"));
                return;
            }

            CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
            // gets the next single threaded executor from the list of executors
            ExecutorService listenerThread = externalExecutorProvider.getExecutor();
            ReaderImpl<T> reader = new ReaderImpl<>(PulsarClientImpl.this, conf, listenerThread, consumerSubscribedFuture, schema);

            synchronized (consumers) {
                consumers.put(reader.getConsumer(), Boolean.TRUE);
            }

            consumerSubscribedFuture.thenRun(() -> {
                readerFuture.complete(reader);
            }).exceptionally(ex -> {
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
                    .failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name: " + topic));
        }

        return lookup.getSchema(topicName);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        log.info("Client closing. URL: {}", lookup.getServiceUrl());
        if (!state.compareAndSet(State.Open, State.Closing)) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = Lists.newArrayList();

        synchronized (producers) {
            // Copy to a new list, because the closing will trigger a removal from the map
            // and invalidate the iterator
            List<ProducerBase<?>> producersToClose = Lists.newArrayList(producers.keySet());
            producersToClose.forEach(p -> futures.add(p.closeAsync()));
        }

        synchronized (consumers) {
            List<ConsumerBase<?>> consumersToClose = Lists.newArrayList(consumers.keySet());
            consumersToClose.forEach(c -> futures.add(c.closeAsync()));
        }

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
            lookup.close();
            cnxPool.close();
            timer.stop();
            externalExecutorProvider.shutdownNow();
            conf.getAuthentication().close();
        } catch (Throwable t) {
            log.warn("Failed to shutdown Pulsar client", t);
            throw PulsarClientException.unwrap(t);
        }
    }

    @Override
    public synchronized void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        log.info("Updating service URL to {}", serviceUrl);

        conf.setServiceUrl(serviceUrl);
        lookup.updateServiceUrl(serviceUrl);
        cnxPool.closeAllConnections();
    }

    protected CompletableFuture<ClientCnx> getConnection(final String topic) {
        TopicName topicName = TopicName.get(topic);
        return lookup.getBroker(topicName)
                .thenCompose(pair -> cnxPool.getConnection(pair.getLeft(), pair.getRight()));
    }

    /** visible for pulsar-functions **/
    public Timer timer() {
        return timer;
    }

    ExecutorProvider externalExecutorProvider() {
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
            lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.isUseTls(), externalExecutorProvider.getExecutor());
        }
    }

    public CompletableFuture<Integer> getNumberOfPartitions(String topic) {
        return getPartitionedTopicMetadata(topic).thenApply(metadata -> metadata.partitions);
    }

    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(String topic) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();

        try {
            TopicName topicName = TopicName.get(topic);
            AtomicLong opTimeoutMs = new AtomicLong(conf.getOperationTimeoutMs());
            Backoff backoff = new BackoffBuilder()
                    .setInitialTime(100, TimeUnit.MILLISECONDS)
                    .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                    .setMax(1, TimeUnit.MINUTES)
                    .create();
            getPartitionedTopicMetadata(topicName, backoff, opTimeoutMs, metadataFuture);
        } catch (IllegalArgumentException e) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(e.getMessage()));
        }
        return metadataFuture;
    }

    private void getPartitionedTopicMetadata(TopicName topicName,
                                             Backoff backoff,
                                             AtomicLong remainingTime,
                                             CompletableFuture<PartitionedTopicMetadata> future) {
        lookup.getPartitionedTopicMetadata(topicName).thenAccept(future::complete).exceptionally(e -> {
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            // skip retry scheduler when set lookup throttle in client or server side which will lead to `TooManyRequestsException`
            boolean isLookupThrottling = !PulsarClientException.isRetriableError(e.getCause()) || e.getCause() instanceof PulsarClientException.TooManyRequestsException;
            if (nextDelay <= 0 || isLookupThrottling) {
                future.completeExceptionally(e);
                return null;
            }

            ((ScheduledExecutorService) externalExecutorProvider.getExecutor()).schedule(() -> {
                log.warn("[topic: {}] Could not get connection while getPartitionedTopicMetadata -- Will try again in {} ms",
                    topicName, nextDelay);
                remainingTime.addAndGet(-nextDelay);
                getPartitionedTopicMetadata(topicName, backoff, remainingTime, future);
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
        return EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), threadFactory);
    }

    private static ThreadFactory getThreadFactory(String poolName) {
        return new DefaultThreadFactory(poolName, Thread.currentThread().isDaemon());
    }

    void cleanupProducer(ProducerBase<?> producer) {
        synchronized (producers) {
            producers.remove(producer);
        }
    }

    void cleanupConsumer(ConsumerBase<?> consumer) {
        synchronized (consumers) {
            consumers.remove(consumer);
        }
    }

    @VisibleForTesting
    int producersCount() {
        synchronized (producers) {
            return producers.size();
        }
    }

    @VisibleForTesting
    int consumersCount() {
        synchronized (consumers) {
            return consumers.size();
        }
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

    private LoadingCache<String, SchemaInfoProvider> getSchemaProviderLoadingCache() {
        return schemaProviderLoadingCache;
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
                Schema finalSchema = schema;
                return schemaInfoProvider.getLatestSchema().thenCompose(schemaInfo -> {
                    if (null == schemaInfo) {
                        if (!(finalSchema instanceof AutoConsumeSchema)) {
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

    //
    // Transaction related API
    //

    // This method should be exposed in the PulsarClient interface. Only expose it when all the transaction features
    // are completed.
    // @Override
    public TransactionBuilder newTransaction() {
        return new TransactionBuilderImpl(this);
    }

}
