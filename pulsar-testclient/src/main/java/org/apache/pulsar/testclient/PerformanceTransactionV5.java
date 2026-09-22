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
import org.apache.pulsar.cli.converters.picocli.EnumNameConverter;
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
import org.apache.pulsar.testclient.PerformanceConsumer.SubscriptionType;

/**
 * Runs the {@code transaction} benchmark with the V5 client API against the scalable-topics
 * transaction coordinator.
 *
 * <p>Used for {@code topic://} (scalable) topics, and for any topics with {@code --client-api V5}.
 * Everything that is not V5-specific lives in {@link PerformanceTransactionBase}.
 */
public class PerformanceTransactionV5 extends PerformanceTransactionBase<PulsarClient, AsyncProducer<byte[]>,
        QueueConsumer<byte[]>, Message<byte[]>, Transaction> {

    private final SubscriptionInitialPosition subscriptionInitialPosition;

    public PerformanceTransactionV5(PerformanceTransaction arguments) {
        super(arguments);
        this.subscriptionInitialPosition =
                EnumNameConverter.mapByName(arguments.subscriptionInitialPosition, SubscriptionInitialPosition.class);
    }

    @Override
    protected void createTopicsIfNeeded() throws Exception {
        if (!arguments.v5.scalable) {
            super.createTopicsIfNeeded();
            return;
        }
        // Scalable topics (PIP-473) must be pre-created via the admin API — they don't
        // auto-create on produce. Create both the produce and consume topics so a
        // transaction against the scalable-topics coordinator has segment participants.
        final PulsarAdminBuilder adminBuilder = PerfClientUtils
                .createAdminBuilderFromArguments(arguments, arguments.adminURL);
        try (PulsarAdmin adminClient = adminBuilder.build()) {
            List<String> allTopics = new ArrayList<>(arguments.producerTopic);
            allTopics.addAll(arguments.consumerTopic);
            for (String topic : allTopics) {
                try {
                    adminClient.scalableTopics().createScalableTopic(topic, arguments.v5.scalableSegments);
                    log.info().attr("topic", topic).attr("segments", arguments.v5.scalableSegments)
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
        if (arguments.subscriptionType == SubscriptionType.Exclusive
                || arguments.subscriptionType == SubscriptionType.Failover) {
            log.warn().attr("type", arguments.subscriptionType)
                    .log("V5 has no exclusive/failover subscription type. Falling back to QueueConsumer "
                            + "(Shared-style work distribution). Use persistent:// topics or --client-api V4 for the "
                            + "v4 client.");
        }
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        PulsarClientBuilder clientBuilder = PerfClientUtils.createV5ClientBuilderFromArguments(arguments);
        if (!arguments.isDisableTransaction) {
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
                .receiverQueueSize(arguments.receiverQueueSize)
                .subscriptionInitialPosition(this.subscriptionInitialPosition)
                .replicateSubscriptionState(arguments.replicatedSubscription)
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
