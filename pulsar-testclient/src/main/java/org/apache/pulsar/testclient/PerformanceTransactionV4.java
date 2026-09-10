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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The {@code transaction} benchmark driven by the v4 ({@code pulsar-client-original}) client and
 * the v4 transaction coordinator, which stays a live broker code path alongside the v5 one
 * (PIP-473 P5.4) but has no other perf command driving it.
 *
 * <p>This is the counterpart of {@link PerformanceTransaction}: the benchmark itself comes from
 * {@link PerformanceTransactionBase} and only the client bindings differ. Two things it measures
 * that the V5 command cannot: the acknowledgement round trip (v4 {@code acknowledgeAsync} completes
 * when the broker has replied, whereas V5's {@code acknowledge} is a synchronous void), and real
 * {@code Exclusive}/{@code Failover}/{@code Key_Shared} subscription types.
 */
@Command(name = "transaction-v4",
        description = "Test pulsar transaction performance using the v4 client.")
public class PerformanceTransactionV4 extends PerformanceTransactionBase<PulsarClient, Producer<byte[]>,
        Consumer<byte[]>, Message<byte[]>, Transaction> {

    @Option(names = {"-sp", "--subscription-position"}, description = "Subscription position")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Earliest;

    public PerformanceTransactionV4() {
        super("transaction-v4");
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(this)
                .enableTransaction(!this.isDisableTransaction);
        return clientBuilder.build();
    }

    @Override
    protected void closeClient(PulsarClient client) {
        PerfClientUtils.closeClient(client);
    }

    @Override
    protected CompletableFuture<Producer<byte[]>> createProducerAsync(PulsarClient client, String topic) {
        return client.newProducer(Schema.BYTES)
                // The v4 client has allowed a send timeout inside a transaction since #16519, but a
                // timeout still fails the send on its own schedule and takes the transaction with
                // it. Disable it so --txn-timeout is the only deadline.
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topic)
                .createAsync();
    }

    @Override
    protected CompletableFuture<Consumer<byte[]>> subscribeAsync(PulsarClient client, String topic,
                                                                 String subscription) {
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer(Schema.BYTES)
                .subscriptionType(
                        org.apache.pulsar.client.api.SubscriptionType.valueOf(this.subscriptionType.name()))
                .receiverQueueSize(this.receiverQueueSize)
                .subscriptionInitialPosition(this.subscriptionInitialPosition)
                .replicateSubscriptionState(this.replicatedSubscription)
                .topic(topic)
                .subscriptionName(subscription);
        return consumerBuilder.subscribeAsync();
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
    protected Message<byte[]> receive(Consumer<byte[]> consumer) throws PulsarClientException {
        return consumer.receive();
    }

    @Override
    protected CompletableFuture<Void> acknowledgeAsync(Consumer<byte[]> consumer, Message<byte[]> msg,
                                                       Transaction transaction) {
        return transaction != null
                ? consumer.acknowledgeAsync(msg.getMessageId(), transaction)
                : consumer.acknowledgeAsync(msg);
    }

    @Override
    protected CompletableFuture<?> sendMessage(Producer<byte[]> producer, byte[] payload, Transaction transaction) {
        TypedMessageBuilder<byte[]> msg = transaction != null
                ? producer.newMessage(transaction).value(payload)
                : producer.newMessage().value(payload);
        return msg.sendAsync();
    }

    @Override
    protected boolean isAlreadyClosedException(Throwable cause) {
        return cause instanceof PulsarClientException.AlreadyClosedException;
    }

    /** See {@link PerformanceProducerV4#awaitSendsBeforeEndingTransaction()}. */
    @Override
    protected boolean awaitSendsBeforeEndingTransaction() {
        return false;
    }
}
