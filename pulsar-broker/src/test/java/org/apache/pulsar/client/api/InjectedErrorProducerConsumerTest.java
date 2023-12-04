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
package org.apache.pulsar.client.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class InjectedErrorProducerConsumerTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private String randomTopicName() {
        return BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
    }

    private void sneakyAwait(CountDownLatch countDownLatch) {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private InjectedSubscription createTopicAndSubscriptionAndInjectDispatcher(String topicName, String subscribe,
                                                                               SubscriptionType subscriptionType)
                                                                                throws Exception {
        admin.topics().createNonPartitionedTopic(topicName);
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName(subscribe).receiverQueueSize(5)
                .subscriptionType(subscriptionType).subscribe();
        consumer.close();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, true).join().get();
        PersistentSubscription subscription = persistentTopic.getSubscription(subscribe);
        InjectedSubscription injectedSubscription = new InjectedSubscription(persistentTopic, subscription);
        injectedSubscription.spySubscription();
        return injectedSubscription;
    }

    @Data
    @AllArgsConstructor
    private class InjectedSubscription {
        private PersistentTopic persistentTopic;
        private PersistentSubscription subscription;
        private PersistentSubscription spySubscription;
        private Dispatcher dispatcher;
        private Dispatcher spyDispatcher;
        private AtomicInteger subscribeCounter = new AtomicInteger();
        private HashMap<Integer, CountDownLatch> whichSubscribeTimeout = new HashMap<>();

        private InjectedSubscription(PersistentTopic persistentTopic, PersistentSubscription subscription) {
            this.persistentTopic = persistentTopic;
            this.subscription = subscription;
        }

        private void spySubscription() throws Exception {
            spySubscription = spy(subscription);
            persistentTopic.getSubscriptions().put(subscription.getName(), spySubscription);
            doAnswer(invocation -> {
                int times = subscribeCounter.incrementAndGet();
                org.apache.pulsar.broker.service.Consumer consumer =
                        (org.apache.pulsar.broker.service.Consumer) invocation.getArguments()[0];
                CompletableFuture<Void> future = subscription.addConsumer(consumer);
                return future.thenAccept(ignore -> {
                    CountDownLatch signal = whichSubscribeTimeout.get(times);
                    if (signal != null) {
                        sneakyAwait(signal);
                    }
                });
            }).when(spySubscription).addConsumer(any(org.apache.pulsar.broker.service.Consumer.class));
        }

        private CountDownLatch injectSubscribeControl(int times) {
            CountDownLatch signal = new CountDownLatch(1);
            whichSubscribeTimeout.put(times, signal);
            return signal;
        }
    }

    @Test
    public void testFirstSubscribeTimeout() throws Exception {
        final String topicName = randomTopicName();
        final String subscribeName = "subscribe1";
        final SubscriptionType subscriptionType = SubscriptionType.Shared;
        final int pulsarClientTimeoutSeconds = 2;
        PulsarClient lowTimeoutClient = PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .operationTimeout(pulsarClientTimeoutSeconds, TimeUnit.SECONDS)
                .statsInterval(0, TimeUnit.SECONDS).build();

        // Inject a timeout.
        InjectedSubscription injectedSubscription =
                createTopicAndSubscriptionAndInjectDispatcher(topicName, subscribeName, subscriptionType);
        CountDownLatch subscribeSignal = injectedSubscription.injectSubscribeControl(1);

        // Do subscribe.
        try {
            lowTimeoutClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subscribeName).receiverQueueSize(5)
                    .subscriptionType(subscriptionType).subscribe();
            fail("expected the consumer1 creation will timeout.");
        } catch (Exception ex) {
            //
        }
        subscribeSignal.countDown();

        // Verify the consumer will be removed from dispatcher.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(injectedSubscription.subscription.getDispatcher().getConsumers().size(), 0);
        });

        // cleanup.
        lowTimeoutClient.close();
        admin.topics().delete(topicName, false);
    }
}
