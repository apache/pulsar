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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncMessageBuilder;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * A client program to test pulsar transaction performance with the V5 client API.
 *
 * <p>Everything that is not V5-specific lives in {@link PerformanceTransactionBase}; the v4 client
 * and its transaction coordinator are driven by {@link PerformanceTransactionV4} under the
 * {@code transaction-v4} name.
 */
@Command(name = "transaction", description = "Test pulsar transaction performance.")
public class PerformanceTransaction extends PerformanceTransactionBase<PulsarClient, AsyncProducer<byte[]>,
        QueueConsumer<byte[]>, Message<byte[]>, Transaction> {

    @Option(names = {"--scalable"}, description = "Create the producer/consumer topics as scalable"
            + " topics (PIP-473) with --scalable-segments initial segments. Required for transactions"
            + " against the scalable-topics (v5) coordinator. Mutually exclusive with --partitions.")
    public boolean scalable = false;

    @Option(names = {"--scalable-segments"}, description = "Number of initial segments for scalable"
            + " topics created via --scalable.")
    public int scalableSegments = 1;

    @Option(names = {"-sp", "--subscription-position"}, description = "Subscription position")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.EARLIEST;

    public PerformanceTransaction() {
        super("transaction");
    }

    @Override
    protected void createTopicsIfNeeded() throws Exception {
        if (!this.scalable) {
            super.createTopicsIfNeeded();
            return;
        }
        // Scalable topics (PIP-473) must be pre-created via the admin API — they don't
        // auto-create on produce. Create both the produce and consume topics so a
        // transaction against the scalable-topics coordinator has segment participants.
        final PulsarAdminBuilder adminBuilder = PerfClientUtils
                .createAdminBuilderFromArguments(this, this.adminURL);
        try (PulsarAdmin adminClient = adminBuilder.build()) {
            List<String> allTopics = new ArrayList<>(this.producerTopic);
            allTopics.addAll(this.consumerTopic);
            for (String topic : allTopics) {
                try {
                    adminClient.scalableTopics().createScalableTopic(topic, this.scalableSegments);
                    log.info().attr("topic", topic).attr("segments", this.scalableSegments)
                            .log("Created scalable topic");
                } catch (PulsarAdminException.ConflictException alreadyExists) {
                    log.debug().attr("topic", topic).attr("exists", alreadyExists)
                            .log("Scalable topic already exists");
                }
            }
        }
    }

    @Override
    protected void prepareRun() {
        if (this.subscriptionType == SubscriptionType.Exclusive
                || this.subscriptionType == SubscriptionType.Failover) {
            log.warn().attr("type", this.subscriptionType)
                    .log("V5 has no exclusive/failover subscription type. Falling back to QueueConsumer "
                            + "(Shared-style work distribution). Use transaction-v4 for the v4 client.");
        }
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        PulsarClientBuilder clientBuilder = PerfClientUtils.createV5ClientBuilderFromArguments(this);
        if (!this.isDisableTransaction) {
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

    @Override
    protected CompletableFuture<AsyncProducer<byte[]>> createProducerAsync(PulsarClient client, String topic) {
        return client.newProducer(Schema.bytes())
                .sendTimeout(Duration.ZERO)
                .topic(topic)
                .createAsync()
                .thenApply(Producer::async);
    }

    @Override
    protected CompletableFuture<QueueConsumer<byte[]>> subscribeAsync(PulsarClient client, String topic,
                                                                      String subscription) {
        // V5 QueueConsumerBuilder has no clone(); build fresh per subscription.
        QueueConsumerBuilder<byte[]> b = client.newQueueConsumer(Schema.bytes())
                .receiverQueueSize(this.receiverQueueSize)
                .subscriptionInitialPosition(this.subscriptionInitialPosition)
                .replicateSubscriptionState(this.replicatedSubscription)
                .topic(topic)
                .subscriptionName(subscription);
        return b.subscribeAsync();
    }

    @Override
    protected Transaction newTransaction(PulsarClient client) throws PulsarClientException {
        // Deliberately not PerfClientUtils.newTransactionWithRetry: this command builds its
        // producers and consumers before opening its first transaction, so it does not hit the
        // coordinator-connect race that helper exists for, and the worker's own retry loop has to
        // see - and count - every failed open for -ntxn to terminate.
        return client.newTransaction();
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
    protected Message<byte[]> receive(QueueConsumer<byte[]> consumer) throws PulsarClientException {
        return consumer.receive();
    }

    @Override
    protected CompletableFuture<Void> acknowledgeAsync(QueueConsumer<byte[]> consumer, Message<byte[]> msg,
                                                       Transaction transaction) {
        // V5 acknowledge is synchronous void, so the reported ack latency is a local measurement
        // rather than the broker round trip the v4 command reports.
        try {
            if (transaction != null) {
                consumer.acknowledge(msg.id(), transaction);
            } else {
                consumer.acknowledge(msg.id());
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    protected CompletableFuture<?> sendMessage(AsyncProducer<byte[]> producer, byte[] payload,
                                               Transaction transaction) {
        AsyncMessageBuilder<byte[]> msg = producer.newMessage().value(payload);
        if (transaction != null) {
            msg.transaction(transaction);
        }
        return msg.send();
    }

    @Override
    protected boolean isAlreadyClosedException(Throwable cause) {
        return cause instanceof PulsarClientException.AlreadyClosedException;
    }
}
