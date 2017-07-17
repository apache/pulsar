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

import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class PulsarClientImpl implements PulsarClient {

    private static final Logger log = LoggerFactory.getLogger(PulsarClientImpl.class);

    private final ClientConfiguration conf;
    private final LookupService lookup;
    private final ConnectionPool cnxPool;
    private final Timer timer;
    private final ExecutorProvider externalExecutorProvider;

    enum State {
        Open, Closing, Closed
    }

    private AtomicReference<State> state = new AtomicReference<>();
    private final IdentityHashMap<ProducerBase, Boolean> producers;
    private final IdentityHashMap<ConsumerBase, Boolean> consumers;

    private final AtomicLong producerIdGenerator = new AtomicLong();
    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong requestIdGenerator = new AtomicLong();

    private final EventLoopGroup eventLoopGroup;

    public PulsarClientImpl(String serviceUrl, ClientConfiguration conf) throws PulsarClientException {
        this(serviceUrl, conf, getEventLoopGroup(conf));
    }

    public PulsarClientImpl(String serviceUrl, ClientConfiguration conf, EventLoopGroup eventLoopGroup)
            throws PulsarClientException {
        if (serviceUrl == null || conf == null || eventLoopGroup == null) {
            throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
        }
        this.eventLoopGroup = eventLoopGroup;
        this.conf = conf;
        conf.getAuthentication().start();
        cnxPool = new ConnectionPool(this, eventLoopGroup);
        if (serviceUrl.startsWith("http")) {
            lookup = new HttpLookupService(serviceUrl, conf, eventLoopGroup);
        } else {
            lookup = new BinaryProtoLookupService(this, serviceUrl, conf.isUseTls());
        }
        timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-timer"), 1, TimeUnit.MILLISECONDS);
        externalExecutorProvider = new ExecutorProvider(conf.getListenerThreads(), "pulsar-external-listener");
        producers = Maps.newIdentityHashMap();
        consumers = Maps.newIdentityHashMap();
        state.set(State.Open);
    }

    public ClientConfiguration getConfiguration() {
        return conf;
    }

    @Override
    public Producer createProducer(String destination) throws PulsarClientException {
        try {
            return createProducerAsync(destination, new ProducerConfiguration()).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public Producer createProducer(final String destination, final ProducerConfiguration conf)
            throws PulsarClientException {
        try {
            return createProducerAsync(destination, conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Producer> createProducerAsync(String topic) {
        return createProducerAsync(topic, new ProducerConfiguration());
    }

    @Override
    public CompletableFuture<Producer> createProducerAsync(final String topic, final ProducerConfiguration conf) {
        return createProducerAsync(topic, conf, null);
    }

    public CompletableFuture<Producer> createProducerAsync(final String topic, final ProducerConfiguration conf,
            String producerName) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (!DestinationName.isValid(topic)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
        }
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Producer configuration undefined"));
        }

        CompletableFuture<Producer> producerCreatedFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ProducerBase producer;
            if (metadata.partitions > 1) {
                producer = new PartitionedProducerImpl(PulsarClientImpl.this, topic, conf, metadata.partitions,
                        producerCreatedFuture);
            } else {
                producer = new ProducerImpl(PulsarClientImpl.this, topic, producerName, conf, producerCreatedFuture,
                        -1);
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

    @Override
    public Consumer subscribe(final String topic, final String subscription) throws PulsarClientException {
        return subscribe(topic, subscription, new ConsumerConfiguration());
    }

    @Override
    public Consumer subscribe(String topic, String subscription, ConsumerConfiguration conf)
            throws PulsarClientException {
        try {
            return subscribeAsync(topic, subscription, conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Consumer> subscribeAsync(String topic, String subscription) {
        return subscribeAsync(topic, subscription, new ConsumerConfiguration());
    }

    @Override
    public CompletableFuture<Consumer> subscribeAsync(final String topic, final String subscription,
            final ConsumerConfiguration conf) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }
        if (!DestinationName.isValid(topic)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
        }
        if (isBlank(subscription)) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Empty subscription name"));
        }
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        CompletableFuture<Consumer> consumerSubscribedFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ConsumerBase consumer;
            // gets the next single threaded executor from the list of executors
            ExecutorService listenerThread = externalExecutorProvider.getExecutor();
            if (metadata.partitions > 1) {
                consumer = new PartitionedConsumerImpl(PulsarClientImpl.this, topic, subscription, conf,
                        metadata.partitions, listenerThread, consumerSubscribedFuture);
            } else {
                consumer = new ConsumerImpl(PulsarClientImpl.this, topic, subscription, conf, listenerThread, -1,
                        consumerSubscribedFuture);
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

    @Override
    public Reader createReader(String topic, MessageId startMessageId, ReaderConfiguration conf)
            throws PulsarClientException {
        try {
            return createReaderAsync(topic, startMessageId, conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Reader> createReaderAsync(String topic, MessageId startMessageId,
            ReaderConfiguration conf) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }
        if (!DestinationName.isValid(topic)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
        }
        if (startMessageId == null) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Invalid startMessageId"));
        }
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        CompletableFuture<Reader> readerFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            if (metadata.partitions > 1) {
                readerFuture.completeExceptionally(
                        new PulsarClientException("Topic reader cannot be created on a partitioned topic"));
                return;
            }

            CompletableFuture<Consumer> consumerSubscribedFuture = new CompletableFuture<>();
            // gets the next single threaded executor from the list of executors
            ExecutorService listenerThread = externalExecutorProvider.getExecutor();
            ReaderImpl reader = new ReaderImpl(PulsarClientImpl.this, topic, startMessageId, conf, listenerThread,
                    consumerSubscribedFuture);

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

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            throw new PulsarClientException(e);
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
            List<ProducerBase> producersToClose = Lists.newArrayList(producers.keySet());
            producersToClose.forEach(p -> futures.add(p.closeAsync()));
        }

        synchronized (consumers) {
            List<ConsumerBase> consumersToClose = Lists.newArrayList(consumers.keySet());
            consumersToClose.forEach(c -> futures.add(c.closeAsync()));
        }

        FutureUtil.waitForAll(futures).thenRun(() -> {
            // All producers & consumers are now closed, we can stop the client safely
            try {
                shutdown();
                closeFuture.complete(null);
                state.set(State.Closed);
            } catch (PulsarClientException e) {
                closeFuture.completeExceptionally(e);
            }
        }).exceptionally(exception -> {
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
            throw new PulsarClientException(t);
        }
    }

    protected CompletableFuture<ClientCnx> getConnection(final String topic) {
        DestinationName destinationName = DestinationName.get(topic);
        return lookup.getBroker(destinationName)
                .thenCompose(pair -> cnxPool.getConnection(pair.getLeft(), pair.getRight()));
    }

    protected Timer timer() {
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

    EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    private CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(String topic) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture;

        try {
            DestinationName destinationName = DestinationName.get(topic);
            metadataFuture = lookup.getPartitionedTopicMetadata(destinationName);
        } catch (IllegalArgumentException e) {
            return FutureUtil.failedFuture(e);
        }
        return metadataFuture;
    }

    private static EventLoopGroup getEventLoopGroup(ClientConfiguration conf) {
        int numThreads = conf.getIoThreads();
        ThreadFactory threadFactory = new DefaultThreadFactory("pulsar-client-io");
        return EventLoopUtil.newEventLoopGroup(numThreads, threadFactory);
    }

    void cleanupProducer(ProducerBase producer) {
        synchronized (producers) {
            producers.remove(producer);
        }
    }

    void cleanupConsumer(ConsumerBase consumer) {
        synchronized (consumers) {
            consumers.remove(consumer);
        }
    }
}
