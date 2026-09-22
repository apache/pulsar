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
import io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.cli.converters.picocli.EnumNameConverter;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.StreamConsumerBuilder;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.auth.PemFileKeyProvider;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.testclient.PerformanceConsumer.ScalableConsumerType;
import org.apache.pulsar.testclient.PerformanceConsumer.SubscriptionType;

/**
 * Runs the {@code consume} benchmark with the V5 client API.
 *
 * <p>Used for {@code topic://} (scalable) topics, and for any topic with {@code --client-api V5}.
 * Everything that is not V5-specific lives in {@link PerformanceConsumerBase}.
 */
public class PerformanceConsumerV5 extends
        PerformanceConsumerBase<PulsarClient, PerformanceConsumerV5.PerfConsumer, Message<byte[]>, Transaction> {

    private final SubscriptionInitialPosition subscriptionInitialPosition;
    private ConsumerEncryptionPolicy encryptionPolicy;
    private ExecutorService consumerExec;

    public PerformanceConsumerV5(PerformanceConsumer arguments) {
        super(arguments);
        this.subscriptionInitialPosition =
                EnumNameConverter.mapByName(arguments.subscriptionInitialPosition, SubscriptionInitialPosition.class);
    }

    @Override
    protected Object consumerTypeForLog() {
        return arguments.v5.scalableConsumerType;
    }

    @Override
    protected void prepareRun() {
        log.info().attr("consumerType", arguments.v5.scalableConsumerType)
                .log("Using V5 scalable-topic consumer API");
        if (arguments.subscriptionType == SubscriptionType.Exclusive
                || arguments.subscriptionType == SubscriptionType.Failover) {
            log.warn().attr("type", arguments.subscriptionType)
                    .log("V5 has no exclusive/failover subscription type. Falling back to QueueConsumer "
                            + "(Shared-style work distribution). Latency/throughput numbers may not be "
                            + "directly comparable with the v4 client. Use a persistent:// topic or "
                            + "--client-api V4 for the v4 client.");
        }
        this.encryptionPolicy = buildEncryptionPolicyOrNull();
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

    @Override
    protected CompletableFuture<PerfConsumer> subscribeAsync(PulsarClient client, String topic,
                                                             String subscription) {
        if (arguments.v5.scalableConsumerType == ScalableConsumerType.Stream) {
            // StreamConsumer has no receiverQueueSize knob; the rest carries over. Deliberately
            // do NOT set a consumerName: the controller keys group membership by consumer name,
            // so the V5 client's auto-generated unique name keeps every consumer — within one
            // process and across separate `pulsar-perf consume` invocations — a distinct member.
            // (Setting a deterministic name would make two processes collide and the second be
            // treated as a reconnect of the first.)
            StreamConsumerBuilder<byte[]> b = client.newStreamConsumer(Schema.bytes())
                    .acknowledgmentGroupTime(Duration.ofMillis(arguments.acknowledgmentsGroupingDelayMillis))
                    .subscriptionInitialPosition(this.subscriptionInitialPosition)
                    .replicateSubscriptionState(arguments.replicatedSubscription)
                    .topic(topic)
                    .subscriptionName(subscription);
            if (encryptionPolicy != null) {
                b.encryptionPolicy(encryptionPolicy);
            }
            return b.subscribeAsync().thenApply(PerformanceConsumerV5::wrap);
        }
        QueueConsumerBuilder<byte[]> b = client.newQueueConsumer(Schema.bytes())
                .receiverQueueSize(arguments.receiverQueueSize)
                .acknowledgmentGroupTime(Duration.ofMillis(arguments.acknowledgmentsGroupingDelayMillis))
                .subscriptionInitialPosition(this.subscriptionInitialPosition)
                .replicateSubscriptionState(arguments.replicatedSubscription)
                .topic(topic)
                .subscriptionName(subscription);
        if (encryptionPolicy != null) {
            b.encryptionPolicy(encryptionPolicy);
        }
        return b.subscribeAsync().thenApply(PerformanceConsumerV5::wrap);
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
    protected int messageSize(Message<byte[]> msg) {
        return msg.size();
    }

    @Override
    protected long publishTimeMillis(Message<byte[]> msg) {
        return msg.publishTime().toEpochMilli();
    }

    @Override
    protected void acknowledge(PerfConsumer consumer, Message<byte[]> msg, Transaction transaction) {
        // V5 acknowledge is synchronous void. Catch any failure into the shared counter.
        try {
            if (transaction != null) {
                consumer.ackTxn(msg.id(), transaction);
            } else {
                consumer.ack(msg.id());
            }
            ackSucceeded();
        } catch (Exception e) {
            ackFailed(e);
        }
    }

    /**
     * V5 has no MessageListener — drive each consumer from a dedicated poll thread that calls
     * receive(timeout) and runs the same per-message handler the v4 listener does. One thread per
     * consumer mirrors the v4 dispatch concurrency closely enough for the perf workload.
     */
    @Override
    protected void startConsuming(List<PerfConsumer> consumers) {
        consumerExec = Executors.newCachedThreadPool(
                new DefaultThreadFactory("pulsar-perf-consumer-poll"));
        for (PerfConsumer consumer : consumers) {
            consumerExec.submit(() -> pollLoop(consumer));
        }
    }

    @Override
    protected void stopConsuming() {
        if (consumerExec == null) {
            return;
        }
        consumerExec.shutdownNow();
        try {
            if (!consumerExec.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Consumer poll executor did not terminate within timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Per-consumer poll loop replacing the v4 {@code MessageListener}. */
    private void pollLoop(PerfConsumer consumer) {
        while (!Thread.currentThread().isInterrupted()) {
            if (checkDone()) {
                return;
            }

            Message<byte[]> msg;
            try {
                msg = consumer.receive(Duration.ofSeconds(1));
            } catch (Exception e) {
                if (PerfClientUtils.hasInterruptedException(e)) {
                    Thread.currentThread().interrupt();
                    return;
                }
                log.warn().exception(e).log("receive failed; retrying");
                continue;
            }
            if (msg == null) {
                continue;
            }

            if (handleMessage(consumer, msg)) {
                return;
            }
        }
    }

    /**
     * Minimal common view over the V5 {@link QueueConsumer} / {@link StreamConsumer} APIs so the
     * poll loop is independent of which scalable-topic consumer type was selected. The ack methods
     * map to {@code acknowledge} for Queue and {@code acknowledgeCumulative} for Stream.
     */
    public interface PerfConsumer {
        Message<byte[]> receive(Duration timeout) throws Exception;

        void ack(MessageId messageId) throws Exception;

        void ackTxn(MessageId messageId, Transaction txn) throws Exception;
    }

    private ConsumerEncryptionPolicy buildEncryptionPolicyOrNull() {
        if (!isNotBlank(arguments.encKeyFile)) {
            return null;
        }
        // We do not know the key name from --encryption-key-value-file alone; PemFileKeyProvider
        // expects a name → path mapping. Register the file under the same name the producer side
        // used (defaults to the file path's last component if unset upstream).
        String keyName = Path.of(arguments.encKeyFile).getFileName().toString();
        PemFileKeyProvider keys = PemFileKeyProvider.builder()
                .privateKey(keyName, Path.of(arguments.encKeyFile))
                .build();
        return ConsumerEncryptionPolicy.builder()
                .privateKeyProvider(keys)
                .build();
    }

    private static PerfConsumer wrap(QueueConsumer<byte[]> consumer) {
        return new PerfConsumer() {
            @Override
            public Message<byte[]> receive(Duration timeout) throws Exception {
                return consumer.receive(timeout);
            }

            @Override
            public void ack(MessageId messageId) throws Exception {
                consumer.acknowledge(messageId);
            }

            @Override
            public void ackTxn(MessageId messageId, Transaction txn) throws Exception {
                consumer.acknowledge(messageId, txn);
            }
        };
    }

    private static PerfConsumer wrap(StreamConsumer<byte[]> consumer) {
        return new PerfConsumer() {
            @Override
            public Message<byte[]> receive(Duration timeout) throws Exception {
                return consumer.receive(timeout);
            }

            @Override
            public void ack(MessageId messageId) throws Exception {
                consumer.acknowledgeCumulative(messageId);
            }

            @Override
            public void ackTxn(MessageId messageId, Transaction txn) throws Exception {
                consumer.acknowledgeCumulative(messageId, txn);
            }
        };
    }
}
