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
import org.apache.pulsar.cli.converters.picocli.EnumNameConverter;
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

/**
 * Runs the {@code produce} benchmark with the V5 client API.
 *
 * <p>Used for {@code topic://} (scalable) topics, and for any topic with {@code --client-api V5}. Everything
 * that is not V5-specific lives in {@link PerformanceProducerBase}.
 */
public class PerformanceProducerV5
        extends PerformanceProducerBase<PulsarClient, AsyncProducer<byte[]>, Transaction> {

    public PerformanceProducerV5(PerformanceProducer arguments) {
        super(arguments);
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        PulsarClientBuilder clientBuilder = PerfClientUtils.createV5ClientBuilderFromArguments(arguments);
        if (arguments.isEnableTransaction) {
            clientBuilder.transactionPolicy(TransactionPolicy.builder()
                    .timeout(Duration.ofSeconds(arguments.transactionTimeout))
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
                .sendTimeout(Duration.ofSeconds(arguments.sendTimeout))
                .compressionPolicy(CompressionPolicy.of(
                        EnumNameConverter.mapByName(arguments.compression, CompressionType.class)))
                .accessMode(EnumNameConverter.mapByName(arguments.producerAccessMode, ProducerAccessMode.class))
                .blockIfQueueFull(true);

        // V5 does not expose maxPendingMessages / maxPendingMessagesAcrossPartitions /
        // messageRoutingMode as user-configurable knobs; the SDK manages memory via the
        // client-level MemorySize policy and routes appropriately for regular and scalable
        // topics. --max-outstanding / --max-outstanding-across-partitions are therefore in the
        // command's v4 option group and rejected with this client.

        if (arguments.producerName != null) {
            producerBuilder.producerName(
                    String.format("%s%s%d", arguments.producerName, arguments.separator, producerId));
        }

        // Batching and chunking are mutually exclusive. Chunking wins when both are requested.
        if (arguments.chunkingAllowed) {
            producerBuilder.chunkingPolicy(ChunkingPolicy.builder().enabled(true).build());
            producerBuilder.batchingPolicy(BatchingPolicy.ofDisabled());
        } else if (arguments.disableBatching || (arguments.batchTimeMillis <= 0.0 && arguments.batchMaxMessages <= 0)) {
            producerBuilder.batchingPolicy(BatchingPolicy.ofDisabled());
        } else {
            BatchingPolicy.Builder batching = BatchingPolicy.builder()
                    .enabled(true)
                    .maxPublishDelay(Duration.ofNanos((long) (arguments.batchTimeMillis * 1_000_000)));
            if (arguments.batchMaxMessages > 0) {
                batching.maxMessages(arguments.batchMaxMessages);
            }
            if (arguments.batchMaxBytes > 0) {
                batching.maxSize(MemorySize.ofBytes(arguments.batchMaxBytes));
            }
            producerBuilder.batchingPolicy(batching.build());
        }

        if (isNotBlank(arguments.encKeyName) && isNotBlank(arguments.encKeyFile)) {
            PemFileKeyProvider keyProvider = PemFileKeyProvider.builder()
                    .publicKey(arguments.encKeyName, Path.of(arguments.encKeyFile))
                    .build();
            producerBuilder.encryptionPolicy(ProducerEncryptionPolicy.builder()
                    .publicKeyProvider(keyProvider)
                    .keyName(arguments.encKeyName)
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
        if (arguments.setEventTime) {
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
