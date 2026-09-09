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
package org.apache.pulsar.broker.service.persistent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class PersistentSharedPermitAccountingTest extends SharedPulsarBaseTest {

    private static final int BATCH_SIZE = 10;

    @DataProvider(name = "sharedDispatcherImplementations")
    public Object[][] sharedDispatcherImplementations() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 30000, dataProvider = "sharedDispatcherImplementations")
    public void testBatchDebtIsSharedByConsumerAndDispatcherAndRemovedWithConsumer(boolean classicDispatcher)
            throws Exception {
        boolean originalSetting = getConfig().isSubscriptionSharedUseClassicPersistentImplementation();
        getConfig().setSubscriptionSharedUseClassicPersistentImplementation(classicDispatcher);
        String topicName = newTopicName();
        String subscriptionName = "shared-subscription";

        try (org.apache.pulsar.client.api.Consumer<Integer> clientConsumer =
                     pulsarClient.newConsumer(Schema.INT32)
                             .topic(topicName)
                             .subscriptionName(subscriptionName)
                             .subscriptionType(SubscriptionType.Shared)
                             .receiverQueueSize(1)
                             .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                             .subscribe();
             Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                     .topic(topicName)
                     .enableBatching(true)
                     .batchingMaxMessages(BATCH_SIZE)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                     .create()) {
            PersistentSubscription subscription = getSubscription(topicName, subscriptionName);

            sendBatch(producer, 0);
            awaitPermitBalances(subscription, 1 - BATCH_SIZE, 1 - BATCH_SIZE, BATCH_SIZE);

            for (int i = 0; i < BATCH_SIZE; i++) {
                Message<Integer> message = clientConsumer.receive(10, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                try {
                    clientConsumer.acknowledge(message);
                } finally {
                    message.release();
                }
            }
            awaitPermitBalances(subscription, 1, 1, 0);

            sendBatch(producer, BATCH_SIZE);
            awaitPermitBalances(subscription, 1 - BATCH_SIZE, 1 - BATCH_SIZE, BATCH_SIZE);

            clientConsumer.close();
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Assert.assertTrue(subscription.getConsumers().isEmpty());
                Assert.assertEquals(getTotalAvailablePermits(subscription), 0);
            });
        } finally {
            getConfig().setSubscriptionSharedUseClassicPersistentImplementation(originalSetting);
        }
    }

    private PersistentSubscription getSubscription(String topicName, String subscriptionName) throws Exception {
        PersistentTopic topic = (PersistentTopic) getTopicIfExists(topicName).get(10, TimeUnit.SECONDS)
                .orElseThrow(() -> new IllegalStateException("Topic was not loaded"));
        return topic.getSubscription(subscriptionName);
    }

    private static void sendBatch(Producer<Integer> producer, int firstValue) throws Exception {
        List<CompletableFuture<MessageId>> sends = new ArrayList<>(BATCH_SIZE);
        for (int i = 0; i < BATCH_SIZE; i++) {
            sends.add(producer.sendAsync(firstValue + i));
        }
        producer.flush();
        FutureUtil.waitForAll(sends).get(10, TimeUnit.SECONDS);
    }

    private static void awaitPermitBalances(PersistentSubscription subscription, int consumerPermits,
                                            int dispatcherPermits, int unackedMessages) {
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Assert.assertEquals(subscription.getConsumers().size(), 1);
            Consumer brokerConsumer = subscription.getConsumers().get(0);
            Assert.assertEquals(brokerConsumer.getAvailablePermits(), consumerPermits);
            Assert.assertEquals(brokerConsumer.getUnackedMessages(), unackedMessages);
            Assert.assertEquals(getTotalAvailablePermits(subscription), dispatcherPermits);
        });
    }

    private static int getTotalAvailablePermits(PersistentSubscription subscription) {
        if (subscription.getDispatcher() instanceof PersistentDispatcherMultipleConsumers dispatcher) {
            return dispatcher.totalAvailablePermits;
        }
        if (subscription.getDispatcher() instanceof PersistentDispatcherMultipleConsumersClassic dispatcher) {
            return dispatcher.totalAvailablePermits;
        }
        throw new AssertionError("Unexpected dispatcher " + subscription.getDispatcher());
    }
}
