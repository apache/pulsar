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

import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.CURSOR_INTERNAL_PROPERTY_PREFIX;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.delayed.BucketDelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BucketDelayedDeliveryTest extends DelayedDeliveryTest {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setDelayedDeliveryTrackerFactoryClassName(BucketDelayedDeliveryTrackerFactory.class.getName());
        conf.setDelayedDeliveryMaxNumBuckets(10);
        conf.setDelayedDeliveryMaxTimeStepPerBucketSnapshotSegmentSeconds(1);
        conf.setDelayedDeliveryMaxIndexesPerBucketSnapshotSegment(10);
        conf.setDelayedDeliveryMinIndexCountPerBucket(50);
        conf.setManagedLedgerMaxEntriesPerLedger(50);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testBucketDelayedDeliveryWithAllConsumersDisconnecting() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testDelaysWithAllConsumerDis");

        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .value("msg")
                    .deliverAfter(1, TimeUnit.HOURS)
                    .send();
        }

        Dispatcher dispatcher = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription("sub").getDispatcher();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 1000));
        List<String> bucketKeys =
                ((PersistentDispatcherMultipleConsumers) dispatcher).getCursor().getCursorProperties().keySet().stream()
                        .filter(x -> x.startsWith(ManagedCursorImpl.CURSOR_INTERNAL_PROPERTY_PREFIX)).toList();

        c1.close();

        // Attach a new consumer. Since there are no consumers connected, this will trigger the cursor rewind
        @Cleanup
        Consumer<String> c2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Dispatcher dispatcher2 = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription("sub").getDispatcher();
        List<String> bucketKeys2 =
                ((PersistentDispatcherMultipleConsumers) dispatcher2).getCursor().getCursorProperties().keySet().stream()
                        .filter(x -> x.startsWith(ManagedCursorImpl.CURSOR_INTERNAL_PROPERTY_PREFIX)).toList();

        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher2.getNumberOfDelayedMessages(), 1000));
        Assert.assertEquals(bucketKeys, bucketKeys2);
    }


    @Test
    public void testUnsubscribe() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testUnsubscribes");

        @Cleanup
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .value("msg")
                    .deliverAfter(1, TimeUnit.HOURS)
                    .send();
        }

        Dispatcher dispatcher = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription("sub").getDispatcher();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 1000));

        Map<String, String> cursorProperties =
                ((PersistentDispatcherMultipleConsumers) dispatcher).getCursor().getCursorProperties();
        List<Long> bucketIds = cursorProperties.entrySet().stream()
                .filter(x -> x.getKey().startsWith(CURSOR_INTERNAL_PROPERTY_PREFIX + "delayed.bucket")).map(
                        x -> Long.valueOf(x.getValue())).toList();

        assertTrue(bucketIds.size() > 0);

        c1.close();

        restartBroker();

        admin.topics().deleteSubscription(topic, "sub");

        for (Long bucketId : bucketIds) {
            try {
                LedgerHandle ledgerHandle =
                        pulsarTestContext.getBookKeeperClient()
                                .openLedger(bucketId, BookKeeper.DigestType.CRC32C, new byte[]{});
                Assert.fail("Should fail");
            } catch (BKException.BKNoSuchLedgerExistsException e) {
                // ignore it
            }
        }
    }
}
