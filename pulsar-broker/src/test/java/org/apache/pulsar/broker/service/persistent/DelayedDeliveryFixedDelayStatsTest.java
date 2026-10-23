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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class DelayedDeliveryFixedDelayStatsTest extends ProducerConsumerBase {

    private static final int FIXED_DELAY_DETECTION_LOOKAHEAD = 10;

    @Override
    @BeforeClass
    public void setup() throws Exception {
        conf.setDelayedDeliveryFixedDelayDetectionLookahead(FIXED_DELAY_DETECTION_LOOKAHEAD);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMsgBacklogNoDelayedWhenReadsPausedForFixedDelays() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/fixed-delay-stats");
        final String subName = "sub";
        final int numNotDelayed = 15;
        final int numDelayed = FIXED_DELAY_DETECTION_LOOKAHEAD * 10;

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                // Few permits, so that the dispatcher reads the delayed messages a few at a time
                .receiverQueueSize(numNotDelayed + 5)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();
        for (int i = 0; i < numNotDelayed; i++) {
            producer.send("not-delayed-" + i);
        }
        // The same delay for all messages makes the tracker pause reads once it holds the lookahead
        for (int i = 0; i < numDelayed; i++) {
            producer.newMessage().value("delayed-" + i).deliverAfter(1, TimeUnit.HOURS).send();
        }

        List<Message<String>> received = new ArrayList<>();
        for (int i = 0; i < numNotDelayed; i++) {
            Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
            assertNotNull(msg);
            received.add(msg);
        }

        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                (AbstractPersistentDispatcherMultipleConsumers) persistentTopic.getSubscription(subName)
                        .getDispatcher();
        Awaitility.await().until(dispatcher::isReadPausedByDelayTracker);

        SubscriptionStats stats = admin.topics().getStats(topic).getSubscriptions().get(subName);
        assertEquals(stats.getMsgBacklog(), numNotDelayed + numDelayed);
        // Only the delayed messages read so far are tracked
        assertTrue(stats.getMsgDelayed() >= FIXED_DELAY_DETECTION_LOOKAHEAD, "msgDelayed=" + stats.getMsgDelayed());
        assertTrue(stats.getMsgDelayed() < numDelayed, "msgDelayed=" + stats.getMsgDelayed());
        assertEquals(stats.getMsgBacklogNoDelayed(), numNotDelayed);

        for (Message<String> msg : received) {
            consumer.acknowledge(msg);
        }
        Awaitility.await().untilAsserted(() -> {
            SubscriptionStats s = admin.topics().getStats(topic).getSubscriptions().get(subName);
            assertEquals(s.getMsgBacklog(), numDelayed);
            assertEquals(s.getMsgBacklogNoDelayed(), 0);
        });
    }
}
