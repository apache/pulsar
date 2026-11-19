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
package org.apache.pulsar.testclient;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;

/**
 * Runs the {@code produce} benchmark with the v4 ({@code pulsar-client-original}) client.
 *
 * <p>Used for {@code persistent://}, {@code non-persistent://} and unprefixed topics, and for any topic
 * with {@code --client-api V4}. The benchmark itself — accounting, send loop and reports — comes from
 * {@link PerformanceProducerBase}, and only the client bindings differ. It keeps the v4 producer knobs
 * that have no V5 equivalent working: {@code --max-outstanding},
 * {@code --max-outstanding-across-partitions}, {@code --isolated-clients} and round-robin partition
 * routing.
 */
public class PerformanceProducerV4
        extends PerformanceProducerBase<PulsarClient, Producer<byte[]>, Transaction> {

    private final int isolatedClients;

    private PulsarClientSharedResources sharedResources;

    public PerformanceProducerV4(PerformanceProducer arguments) {
        super(arguments);
        this.isolatedClients = arguments.v4.isolatedClients;
    }

    @Override
    protected int workerCount() {
        return isolatedClients > 0 ? isolatedClients : super.workerCount();
    }

    @Override
    protected int producersForWorker(int workerIndex) {
        if (isolatedClients <= 0) {
            return super.producersForWorker(workerIndex);
        }
        int base = arguments.numProducers / isolatedClients;
        return base + (workerIndex < arguments.numProducers % isolatedClients ? 1 : 0);
    }

    @Override
    protected int producerIdForWorker(int workerIndex, int producerIndex) {
        return isolatedClients > 0 ? workerIndex + producerIndex * isolatedClients : workerIndex;
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(arguments)
                .enableTransaction(arguments.isEnableTransaction);
        if (sharedResources != null) {
            clientBuilder.sharedResources(sharedResources);
        }
        return clientBuilder.build();
    }

    @Override
    protected void prepareRun() {
        sharedResources = PulsarClientSharedResources.builder()
                .resourceTypes(PulsarClientSharedResources.SharedResource.EventLoopGroup,
                        PulsarClientSharedResources.SharedResource.ListenerExecutor,
                        PulsarClientSharedResources.SharedResource.InternalExecutor,
                        PulsarClientSharedResources.SharedResource.ScheduledExecutor,
                        PulsarClientSharedResources.SharedResource.LookupExecutor,
                        PulsarClientSharedResources.SharedResource.Timer,
                        PulsarClientSharedResources.SharedResource.DnsResolver)
                .configureEventLoop(config -> config.numberOfThreads(arguments.ioThreads)
                        .enableBusyWait(arguments.enableBusyWait))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.ListenerExecutor,
                        config -> config.numberOfThreads(arguments.listenerThreads))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.InternalExecutor,
                        config -> config.numberOfThreads(arguments.ioThreads))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.ScheduledExecutor,
                        config -> config.numberOfThreads(arguments.ioThreads))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.LookupExecutor,
                        config -> config.numberOfThreads(1))
                .build();
    }

    @Override
    protected void closeResources() {
        if (sharedResources != null) {
            try {
                sharedResources.close();
            } catch (PulsarClientException e) {
                log.warn().exception(e).log("Failed to close shared client resources");
            } finally {
                sharedResources = null;
            }
        }
    }

    @Override
    protected void closeClient(PulsarClient client) {
        PerfClientUtils.closeClient(client);
    }

    @SuppressWarnings("deprecation")
    ProducerBuilder<byte[]> createProducerBuilder(PulsarClient client, int producerId, String topic) {
        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic(topic)
                .sendTimeout(arguments.sendTimeout, TimeUnit.SECONDS)
                .compressionType(arguments.compression)
                .maxPendingMessages(arguments.v4.maxOutstanding)
                .accessMode(arguments.producerAccessMode)
                // enable round robin message routing if it is a partitioned topic
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        if (arguments.v4.maxPendingMessagesAcrossPartitions > 0) {
            producerBuilder.maxPendingMessagesAcrossPartitions(arguments.v4.maxPendingMessagesAcrossPartitions);
        }

        if (arguments.isEnableTransaction) {
            // The v4 client has allowed a send timeout inside a transaction since #16519, but a
            // timeout still fails the send on its own schedule and takes the transaction with it.
            // Disable it so the transaction timeout is the only deadline.
            producerBuilder.sendTimeout(0, TimeUnit.SECONDS);
        }

        if (arguments.producerName != null) {
            String name = String.format("%s%s%d", arguments.producerName, arguments.separator, producerId);
            producerBuilder.producerName(name);
        }

        if (arguments.disableBatching || (arguments.batchTimeMillis <= 0.0 && arguments.batchMaxMessages <= 0)) {
            producerBuilder.enableBatching(false);
        } else {
            long batchTimeUsec = (long) (arguments.batchTimeMillis * 1000);
            producerBuilder.batchingMaxPublishDelay(batchTimeUsec, TimeUnit.MICROSECONDS).enableBatching(true);
        }
        if (arguments.batchMaxMessages > 0) {
            producerBuilder.batchingMaxMessages(arguments.batchMaxMessages);
        }
        if (arguments.batchMaxBytes > 0) {
            producerBuilder.batchingMaxBytes(arguments.batchMaxBytes);
        }

        // Block if queue is full else we will start seeing errors in sendAsync
        producerBuilder.blockIfQueueFull(true);

        if (isNotBlank(arguments.encKeyName) && isNotBlank(arguments.encKeyFile)) {
            producerBuilder.addEncryptionKey(arguments.encKeyName);
            producerBuilder.defaultCryptoKeyReader(arguments.encKeyFile);
        }

        // Chunking and batching are mutually exclusive; chunking wins when both are requested.
        if (arguments.chunkingAllowed) {
            producerBuilder.enableChunking(true);
            producerBuilder.enableBatching(false);
        }

        return producerBuilder;
    }

    @Override
    protected CompletableFuture<Producer<byte[]>> createProducerAsync(PulsarClient client, int producerId,
                                                                      String topic) {
        return createProducerBuilder(client, producerId, topic).createAsync();
    }

    @Override
    protected Transaction newTransaction(PulsarClient client) throws Exception {
        return client.newTransaction()
                .withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS)
                .build()
                .get();
    }

    @Override
    protected CompletableFuture<Void> commitTransaction(Transaction transaction) {
        return transaction.commit();
    }

    @Override
    protected CompletableFuture<Void> abortTransaction(Transaction transaction) {
        return transaction.abort();
    }

    @Override
    protected CompletableFuture<?> sendMessage(Producer<byte[]> producer, byte[] payload, Transaction transaction,
                                               String key, Long deliverAfterSeconds) {
        TypedMessageBuilder<byte[]> messageBuilder = transaction != null
                ? producer.newMessage(transaction).value(payload)
                : producer.newMessage().value(payload);
        if (deliverAfterSeconds != null) {
            messageBuilder.deliverAfter(deliverAfterSeconds, TimeUnit.SECONDS);
        }
        if (arguments.setEventTime) {
            messageBuilder.eventTime(System.currentTimeMillis());
        }
        if (key != null) {
            messageBuilder.key(key);
        }
        return messageBuilder.sendAsync();
    }

    @Override
    protected boolean isAlreadyClosedException(Throwable cause) {
        return cause instanceof PulsarClientException.AlreadyClosedException;
    }

    /**
     * The v4 client registers a transactional send with the transaction coordinator as part of
     * {@code sendAsync} itself, so the commit cannot overtake it the way it can on V5. Not awaiting
     * keeps send and commit pipelined, which is what {@code pulsar-perf produce} measured before the
     * V5 migration.
     */
    @Override
    protected boolean awaitSendsBeforeEndingTransaction() {
        return false;
    }
}
