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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConsumerQueuePermitCompatibilityTest extends SharedPulsarBaseTest {
    private static final int TIMEOUT_SECONDS = 10;
    private static final String SUBSCRIPTION = "queue-permit-compatibility";

    @DataProvider
    public Object[][] startPaused() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "startPaused", timeOut = 30000)
    public void testInitialPermitsSurviveUnsubscribeFailure(boolean startPaused) throws Exception {
        String topic = newTopicName();
        try (PulsarClient client = PulsarClient.builder().serviceUrl(getBrokerServiceUrl()).ioThreads(1).build();
             Producer<String> producer = client.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create();
             ConsumerImpl<String> existingConsumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                     .topic(topic).subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                     .startPaused(true).subscribe()) {
            CompletableFuture<ConsumerImpl<String>> recoveredConsumer = new CompletableFuture<>();
            ClientCnx connection = existingConsumer.getClientCnx();
            // Register the synchronous callback on the only IO thread before it can process the subscribe reply.
            // The unsubscribe changes Ready to Closing before subscription initialization can grant permits.
            connection.ctx().executor().execute(() -> client.newConsumer(Schema.STRING).topic(topic)
                    .subscriptionName(SUBSCRIPTION).subscriptionType(SubscriptionType.Shared)
                    .receiverQueueSize(2).startPaused(startPaused).subscribeAsync()
                    .thenCompose(consumer -> consumer.unsubscribeAsync(false).handle((ignored, error) -> {
                        assertThat(error).isInstanceOf(PulsarClientException.BrokerMetadataException.class);
                        return (ConsumerImpl<String>) consumer;
                    })).whenComplete((consumer, error) -> {
                        if (error == null) {
                            recoveredConsumer.complete(consumer);
                        } else {
                            recoveredConsumer.completeExceptionally(error);
                        }
                    }));
            try (ConsumerImpl<String> consumer = recoveredConsumer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                assertThat(consumer.getClientCnx()).isSameAs(connection);
                assertThat(consumer.isConnected()).isTrue();
                if (startPaused) {
                    consumer.resume();
                }
                producer.send("after-unsubscribe-failure");
                Message<String> message = consumer.receive(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThat(message).as("initial permits after the rejected unsubscribe").isNotNull();
                try {
                    assertThat(message.getValue()).isEqualTo("after-unsubscribe-failure");
                    consumer.acknowledge(message);
                } finally {
                    message.release();
                }
            }
        }
    }

    @Test(timeOut = 60000)
    public void testMultiTopicInitialPermitsReconnectAndSeek() throws Exception {
        String firstTopic = newTopicName();
        String secondTopic = newTopicName();
        try (PulsarClient client = newPulsarClient();
             Producer<String> firstProducer = client.newProducer(Schema.STRING).topic(firstTopic)
                     .enableBatching(false).create();
             Producer<String> secondProducer = client.newProducer(Schema.STRING).topic(secondTopic)
                     .enableBatching(false).create()) {
            List<Producer<String>> producers = List.of(firstProducer, secondProducer);
            List<String> expected = sendRound(producers, "initial");
            try (MultiTopicsConsumerImpl<String> consumer = (MultiTopicsConsumerImpl<String>)
                    client.newConsumer(Schema.STRING).topics(List.of(firstTopic, secondTopic))
                            .subscriptionName(SUBSCRIPTION).receiverQueueSize(2)
                            .isAckReceiptEnabled(true).acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
                assertThat(consumer.getConsumers()).hasSize(2);
                assertThat(receiveRound(consumer, expected.size())).containsExactlyInAnyOrderElementsOf(expected);

                Map<ConsumerImpl<String>, ClientCnx> previousConnections = new HashMap<>();
                consumer.getConsumers().forEach(child -> previousConnections.put(child, child.getClientCnx()));
                List<ClientCnx> connections = consumer.getConsumers().stream()
                        .map(ConsumerImpl::getClientCnx).distinct().toList();
                connections.forEach(ClientCnx::close);
                await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).untilAsserted(() ->
                        previousConnections.forEach((child, previousConnection) -> {
                            assertThat(child.isConnected()).isTrue();
                            assertThat(child.getClientCnx()).isNotNull().isNotSameAs(previousConnection);
                        }));

                List<String> afterReconnect = sendRound(producers, "reconnected");
                assertThat(receiveRound(consumer, afterReconnect.size()))
                        .containsExactlyInAnyOrderElementsOf(afterReconnect);
                expected.addAll(afterReconnect);

                consumer.seek(MessageId.earliest);
                assertThat(receiveRound(consumer, expected.size())).containsExactlyInAnyOrderElementsOf(expected);
            }
        }
    }

    @Test(timeOut = 30000)
    public void testZeroQueuePendingReceiveSurvivesReconnect() throws Exception {
        String topic = newTopicName();
        ExecutorService receiver = Executors.newSingleThreadExecutor();
        try (PulsarClient client = newPulsarClient();
             Producer<String> producer = client.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create();
             ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING).topic(topic)
                     .subscriptionName(SUBSCRIPTION).receiverQueueSize(0).subscribe()) {
            assertThat(consumer).isInstanceOf(ZeroQueueConsumerImpl.class);
            Future<Message<String>> pendingReceive = receiver.submit(() -> {
                return consumer.receive();
            });
            try {
                // The broker seeing the demand proves receive() is waiting before disconnecting.
                await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(brokerConsumer(topic).getAvailablePermits()).isEqualTo(1));
                reconnect(consumer);
                await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(brokerConsumer(topic).getAvailablePermits()).isEqualTo(1));
                assertThat(pendingReceive.isDone()).isFalse();

                producer.send("after-reconnect");
                Message<String> message = pendingReceive.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    assertThat(message.getValue()).isEqualTo("after-reconnect");
                    consumer.acknowledge(message);
                } finally {
                    message.release();
                }
            } finally {
                pendingReceive.cancel(true);
            }
        } finally {
            receiver.shutdownNow();
            assertThat(receiver.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(timeOut = 30000)
    public void testZeroQueueDoesNotPrefetchWithoutDemand() throws Exception {
        String topic = newTopicName();
        try (PulsarClient client = newPulsarClient();
             Producer<String> producer = client.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create();
             ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING).topic(topic)
                     .subscriptionName(SUBSCRIPTION).receiverQueueSize(0).subscribe()) {
            producer.send("waiting-for-demand");
            assertNoPrefetch(consumer, topic);
            reconnect(consumer);
            assertNoPrefetch(consumer, topic);
        }
    }

    private void assertNoPrefetch(ConsumerImpl<String> consumer, String topic) throws Exception {
        // The reply on the same connection follows any FLOW queued by subscription initialization.
        consumer.getLastMessageIdsAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(consumer.getCurrentReceiverQueueSize()).isZero();
        assertThat(consumer.getAvailablePermits()).isZero();
        assertThat(consumer.numMessagesInQueue()).isZero();
        Consumer brokerConsumer = brokerConsumer(topic);
        assertThat(brokerConsumer.getAvailablePermits()).isZero();
        assertThat(brokerConsumer.getMsgOutCounter()).isZero();
    }

    private Consumer brokerConsumer(String topic) {
        return getTopicReference(topic).orElseThrow().getSubscription(SUBSCRIPTION).getConsumers().get(0);
    }

    private static void reconnect(ConsumerImpl<?> consumer) {
        ClientCnx previousConnection = consumer.getClientCnx();
        consumer.getClientCnx().close();
        await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(consumer.isConnected()).isTrue();
            assertThat(consumer.getClientCnx()).isNotNull().isNotSameAs(previousConnection);
        });
    }

    private static List<String> sendRound(List<Producer<String>> producers, String round) throws Exception {
        List<String> values = new ArrayList<>();
        for (int topicIndex = 0; topicIndex < producers.size(); topicIndex++) {
            for (int messageIndex = 0; messageIndex < 4; messageIndex++) {
                String value = round + "-" + topicIndex + "-" + messageIndex;
                producers.get(topicIndex).send(value);
                values.add(value);
            }
        }
        return values;
    }

    private static List<String> receiveRound(MultiTopicsConsumerImpl<String> consumer, int count) throws Exception {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Message<String> message = consumer.receive(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(message).as("message %s of %s", i + 1, count).isNotNull();
            try {
                values.add(message.getValue());
                // Each round finishes after the broker confirms its acknowledgments, before disconnect or seek.
                consumer.acknowledgeAsync(message).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                message.release();
            }
        }
        return values;
    }
}
