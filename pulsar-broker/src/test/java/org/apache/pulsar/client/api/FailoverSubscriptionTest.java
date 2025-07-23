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

import static org.testng.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class FailoverSubscriptionTest extends ProducerConsumerBase {
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

    @Test(timeOut = 30_000, invocationCount = 5)
    public void testWaitingCursorsCountAfterSwitchingActiveConsumers() throws Exception {
        final String tp = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(tp);
        admin.topics().createSubscription(tp, subscription, MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(tp, false).join().get();
        Map<String, ConsumerImpl<byte[]>> consumerMap = new HashMap<>();
        ConsumerImpl<byte[]> firstConsumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(tp)
                .subscriptionType(SubscriptionType.Failover).subscriptionName(subscription).subscribe();
        consumerMap.put(firstConsumer.getConsumerName(), firstConsumer);
        PersistentDispatcherSingleActiveConsumer dispatcher =
                (PersistentDispatcherSingleActiveConsumer) topic.getSubscription(subscription).getDispatcher();;
        for (int i = 0; i < 100; i++) {
            ConsumerImpl<byte[]> consumerLoop = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(tp)
                    .subscriptionType(SubscriptionType.Failover).subscriptionName(subscription).subscribe();
            consumerMap.put(consumerLoop.getConsumerName(), consumerLoop);
            if (dispatcher != null && dispatcher.getActiveConsumer() != null) {
                String activeConsumer = dispatcher.getActiveConsumer().consumerName();
                consumerMap.remove(activeConsumer).close();
            }
        }

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
        ConcurrentLinkedQueue<ManagedCursorImpl> waitingCursors =
                WhiteboxImpl.getInternalState(managedLedger, "waitingCursors");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(waitingCursors.size(), 1);
        });

        // cleanup.
        for (ConsumerImpl<byte[]> consumer : consumerMap.values()) {
            consumer.close();
        }
        admin.topics().delete(tp, false);
    }
}
