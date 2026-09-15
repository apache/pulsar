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
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncMessageBuilder;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.apache.pulsar.client.api.v5.auth.PemFileKeyProvider;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.ChunkingPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionType;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.config.ProducerAccessMode;
import org.apache.pulsar.client.api.v5.config.ProducerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * A client program to test pulsar producer performance with the V5 client API.
 *
 * <p>The V5 client is used so the command works transparently against both regular and scalable
 * topics. Everything that is not V5-specific lives in {@link PerformanceProducerBase}; the v4
 * client is driven by {@link PerformanceProducerV4} under the {@code produce-v4} name.
 */
@Command(name = "produce", description = "Test pulsar producer performance.")
public class PerformanceProducer
        extends PerformanceProducerBase<PulsarClient, AsyncProducer<byte[]>, Transaction> {

    @Option(names = { "-z", "--compression" }, description = "Compress messages payload")
    public CompressionType compression = CompressionType.NONE;

    @Option(names = { "-am", "--access-mode" }, description = "Producer access mode")
    public ProducerAccessMode producerAccessMode = ProducerAccessMode.SHARED;

    public PerformanceProducer() {
        super("produce");
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        PulsarClientBuilder clientBuilder = PerfClientUtils.createV5ClientBuilderFromArguments(this);
        if (this.isEnableTransaction) {
            clientBuilder.transactionPolicy(TransactionPolicy.builder()
                    .timeout(Duration.ofSeconds(this.transactionTimeout))
                    .build());
        }
        return clientBuilder.build();
    }

    @Override
    protected void closeClient(PulsarClient client) {
        PerfClientUtils.closeClient(client);
    }

    ProducerBuilder<byte[]> createProducerBuilder(PulsarClient client, int producerId, String topic) {
        ProducerBuilder<byte[]> producerBuilder = client.newProducer(Schema.bytes())
                .topic(topic)
                .sendTimeout(Duration.ofSeconds(this.sendTimeout))
                .compressionPolicy(CompressionPolicy.of(this.compression))
                .accessMode(this.producerAccessMode)
                .blockIfQueueFull(true);

        // V5 does not expose maxPendingMessages / maxPendingMessagesAcrossPartitions /
        // messageRoutingMode as user-configurable knobs; the SDK manages memory via the
        // client-level MemorySize policy and routes appropriately for regular and scalable
        // topics. The legacy --max-outstanding / --max-outstanding-across-partitions flags
        // are accepted for back-compat but have no effect on the V5 client. Use produce-v4
        // to exercise them.

        if (this.producerName != null) {
            producerBuilder.producerName(String.format("%s%s%d", this.producerName, this.separator, producerId));
        }

        // Batching and chunking are mutually exclusive. Chunking wins when both are requested.
        if (this.chunkingAllowed) {
            producerBuilder.chunkingPolicy(ChunkingPolicy.builder().enabled(true).build());
            producerBuilder.batchingPolicy(BatchingPolicy.ofDisabled());
        } else if (this.disableBatching || (this.batchTimeMillis <= 0.0 && this.batchMaxMessages <= 0)) {
            producerBuilder.batchingPolicy(BatchingPolicy.ofDisabled());
        } else {
            BatchingPolicy.Builder batching = BatchingPolicy.builder()
                    .enabled(true)
                    .maxPublishDelay(Duration.ofNanos((long) (this.batchTimeMillis * 1_000_000)));
            if (this.batchMaxMessages > 0) {
                batching.maxMessages(this.batchMaxMessages);
            }
            if (this.batchMaxBytes > 0) {
                batching.maxSize(MemorySize.ofBytes(this.batchMaxBytes));
            }
            producerBuilder.batchingPolicy(batching.build());
        }

        if (isNotBlank(this.encKeyName) && isNotBlank(this.encKeyFile)) {
            PemFileKeyProvider keyProvider = PemFileKeyProvider.builder()
                    .publicKey(this.encKeyName, Path.of(this.encKeyFile))
                    .build();
            producerBuilder.encryptionPolicy(ProducerEncryptionPolicy.builder()
                    .publicKeyProvider(keyProvider)
                    .keyName(this.encKeyName)
                    .build());
        }

        return producerBuilder;
    }

    @Override
    protected CompletableFuture<AsyncProducer<byte[]>> createProducerAsync(PulsarClient client, int producerId,
                                                                           String topic) {
        return createProducerBuilder(client, producerId, topic).createAsync().thenApply(Producer::async);
    }

    @Override
    protected Transaction newTransaction(PulsarClient client) throws PulsarClientException {
        return client.newTransaction();
    }

    @Override
    protected Transaction openFirstTransaction(PulsarClient client)
            throws PulsarClientException, InterruptedException {
        return PerfClientUtils.newTransactionWithRetry(client);
    }

    @Override
    protected CompletableFuture<Void> commitTransaction(Transaction transaction) {
        return transaction.async().commit();
    }

    @Override
    protected CompletableFuture<Void> abortTransaction(Transaction transaction) {
        return transaction.async().abort();
    }

    @Override
    protected CompletableFuture<?> sendMessage(AsyncProducer<byte[]> producer, byte[] payload,
                                               Transaction transaction, String key, Long deliverAfterSeconds) {
        AsyncMessageBuilder<byte[]> messageBuilder = producer.newMessage().value(payload);
        if (transaction != null) {
            messageBuilder.transaction(transaction);
        }
        if (deliverAfterSeconds != null) {
            messageBuilder.deliverAfter(Duration.ofSeconds(deliverAfterSeconds));
        }
        if (this.setEventTime) {
            messageBuilder.eventTime(Instant.now());
        }
        if (key != null) {
            messageBuilder.key(key);
        }
        return messageBuilder.send();
    }

    @Override
    protected boolean isAlreadyClosedException(Throwable cause) {
        return cause instanceof PulsarClientException.AlreadyClosedException;
    }
}
