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
package org.apache.pulsar.broker.cache;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public class BacklogConsumerTest extends ProducerConsumerBase {
    private static final int RECEIVE_TIMEOUT_SHORT_MILLIS = 200;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setClusterName("test");
        internalSetup();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @Test(timeOut = 100000, dataProvider = "ackReceiptEnabled")
    public void testDeactivatingBacklogConsumer(boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final long batchMessageDelayMs = 100;
        final int receiverSize = 10;
        final String topicName = "cache-topic";
        final String topic = "persistent://my-property/my-ns/" + topicName;
        final String sub1 = "faster-sub1";
        final String sub2 = "slower-sub2";

        // 1. Subscriber Faster subscriber: let it consume all messages immediately
        @Cleanup
        Consumer<byte[]> subscriber1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/" + topicName).subscriptionName(sub1)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(receiverSize).subscribe();
        // 1.b. Subscriber Slow subscriber:
        @Cleanup
        Consumer<byte[]> subscriber2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/" + topicName).subscriptionName(sub2)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(receiverSize).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topic);
        producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(5);
        @Cleanup
        Producer<byte[]> producer = producerBuilder.create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topicRef.getManagedLedger();

        // reflection to set/get cache-backlog fields value:
        final long maxMessageCacheRetentionTimeMillis = conf.getManagedLedgerCacheEvictionTimeThresholdMillis();
        final long maxActiveCursorBacklogEntries = conf.getManagedLedgerCursorBackloggedThreshold();

        Message<byte[]> msg;
        final int totalMsgs = (int) maxActiveCursorBacklogEntries + receiverSize + 1;
        // 2. Produce messages
        for (int i = 0; i < totalMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        // 3. Consume messages: at Faster subscriber
        for (int i = 0; i < totalMsgs; i++) {
            msg = subscriber1.receive(RECEIVE_TIMEOUT_SHORT_MILLIS, TimeUnit.MILLISECONDS);
            subscriber1.acknowledgeAsync(msg);
        }

        // wait : so message can be eligible to to be evict from cache
        Thread.sleep(maxMessageCacheRetentionTimeMillis);

        // 4. deactivate subscriber which has built the backlog
        topicRef.checkBackloggedCursors();
        Thread.sleep(100);

        // 5. verify: active subscribers
        Set<String> activeSubscriber = new HashSet<>();
        ledger.getActiveCursors().forEach(c -> activeSubscriber.add(c.getName()));
        assertTrue(activeSubscriber.contains(sub1));
        assertFalse(activeSubscriber.contains(sub2));

        // 6. consume messages : at slower subscriber
        for (int i = 0; i < totalMsgs; i++) {
            msg = subscriber2.receive(RECEIVE_TIMEOUT_SHORT_MILLIS, TimeUnit.MILLISECONDS);
            subscriber2.acknowledgeAsync(msg);
        }

        topicRef.checkBackloggedCursors();

        activeSubscriber.clear();
        ledger.getActiveCursors().forEach(c -> activeSubscriber.add(c.getName()));

        assertTrue(activeSubscriber.contains(sub1));
        assertTrue(activeSubscriber.contains(sub2));
    }
}
