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
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The {@code produce} benchmark driven by the v4 ({@code pulsar-client-original}) client.
 *
 * <p>This is the counterpart of {@link PerformanceProducer}: the benchmark itself — options,
 * accounting, send loop and reports — comes from {@link PerformanceProducerBase}, and only the
 * client bindings differ. It exists so the v4 client and v4 (non-scalable) topics can be measured
 * without the V5 SDK in the path, and it keeps the v4 producer knobs that have no V5 equivalent
 * working: {@code --max-outstanding}, {@code --max-outstanding-across-partitions} and
 * round-robin partition routing.
 */
@Command(name = "produce-v4", description = "Test pulsar producer performance using the v4 client.")
public class PerformanceProducerV4
        extends PerformanceProducerBase<PulsarClient, Producer<byte[]>, Transaction> {

    @Option(names = { "-z", "--compression" }, description = "Compress messages payload")
    public CompressionType compression = CompressionType.NONE;

    @Option(names = { "-am", "--access-mode" }, description = "Producer access mode")
    public ProducerAccessMode producerAccessMode = ProducerAccessMode.Shared;

    public PerformanceProducerV4() {
        super("produce-v4");
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(this)
                .enableTransaction(this.isEnableTransaction);
        return clientBuilder.build();
    }

    @Override
    protected void closeClient(PulsarClient client) {
        PerfClientUtils.closeClient(client);
    }

    @SuppressWarnings("deprecation")
    ProducerBuilder<byte[]> createProducerBuilder(PulsarClient client, int producerId, String topic) {
        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic(topic)
                .sendTimeout(this.sendTimeout, TimeUnit.SECONDS)
                .compressionType(this.compression)
                .maxPendingMessages(this.maxOutstanding)
                .accessMode(this.producerAccessMode)
                // enable round robin message routing if it is a partitioned topic
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        if (this.maxPendingMessagesAcrossPartitions > 0) {
            producerBuilder.maxPendingMessagesAcrossPartitions(this.maxPendingMessagesAcrossPartitions);
        }

        if (this.isEnableTransaction) {
            // A send timeout and a transaction are mutually exclusive on the v4 client: a timed-out
            // send would be retried outside the transaction it was issued under.
            producerBuilder.sendTimeout(0, TimeUnit.SECONDS);
        }

        if (this.producerName != null) {
            String name = String.format("%s%s%d", this.producerName, this.separator, producerId);
            producerBuilder.producerName(name);
        }

        if (this.disableBatching || (this.batchTimeMillis <= 0.0 && this.batchMaxMessages <= 0)) {
            producerBuilder.enableBatching(false);
        } else {
            long batchTimeUsec = (long) (this.batchTimeMillis * 1000);
            producerBuilder.batchingMaxPublishDelay(batchTimeUsec, TimeUnit.MICROSECONDS).enableBatching(true);
        }
        if (this.batchMaxMessages > 0) {
            producerBuilder.batchingMaxMessages(this.batchMaxMessages);
        }
        if (this.batchMaxBytes > 0) {
            producerBuilder.batchingMaxBytes(this.batchMaxBytes);
        }

        // Block if queue is full else we will start seeing errors in sendAsync
        producerBuilder.blockIfQueueFull(true);

        if (isNotBlank(this.encKeyName) && isNotBlank(this.encKeyFile)) {
            producerBuilder.addEncryptionKey(this.encKeyName);
            producerBuilder.defaultCryptoKeyReader(this.encKeyFile);
        }

        // Chunking and batching are mutually exclusive; chunking wins when both are requested.
        if (this.chunkingAllowed) {
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
                .withTransactionTimeout(this.transactionTimeout, TimeUnit.SECONDS)
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
        if (this.setEventTime) {
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
