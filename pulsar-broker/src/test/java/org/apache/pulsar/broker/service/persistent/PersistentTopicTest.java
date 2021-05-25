/**
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.RangeSetWrapper;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentTopicTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        resetConfig();
    }

    /**
     * Test validates that broker cleans up topic which failed to unload while bundle unloading.
     *
     * @throws Exception
     */
    @Test
    public void testCleanFailedUnloadTopic() throws Exception {
        final String topicName = "persistent://prop/ns-abc/failedUnload";

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);

        ManagedLedger ml = topicRef.ledger;
        LedgerHandle ledger = mock(LedgerHandle.class);
        Field handleField = ml.getClass().getDeclaredField("currentLedger");
        handleField.setAccessible(true);
        handleField.set(ml, ledger);
        doNothing().when(ledger).asyncClose(any(), any());

        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));
        pulsar.getNamespaceService().unloadNamespaceBundle(bundle, 5, TimeUnit.SECONDS).get();

        retryStrategically((test) -> !pulsar.getBrokerService().getTopicReference(topicName).isPresent(), 5, 500);
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        producer.close();
    }

    @Test
    public void testLruEntryAck() throws Exception {
        final String topicName = "persistent://prop/ns-abc/test" + UUID.randomUUID();
        final int msgNum = 50;
        final String sub = "my-sub";

        conf.setEnableLruCacheMaxUnackedRanges(true);
        restartBroker();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionName("my-sub").subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).enableBatching(false).topic(topicName).create();
        for (int i = 0; i < msgNum; i++) {
            producer.send("msg" + i);
        }
        List<Message<String>> ackedMsg = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(1, TimeUnit.SECONDS);
            if (i % 2 == 0) {
                consumer.acknowledge(msg);
                ackedMsg.add(msg);
            }
        }
        consumer.close();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        PersistentSubscription subscription = topic.getSubscription(sub);
        ManagedCursorImpl cursor = (ManagedCursorImpl) subscription.getCursor();
        assertTrue(cursor.getIndividuallyDeletedMessagesSet() instanceof RangeSetWrapper);
        assertEquals(cursor.getIndividuallyDeletedMessagesSet().toString(), "[(3:1..3:2],(3:3..3:4],(3:5..3:6],(3:7..3:8]]");
        RangeSetWrapper<PositionImpl> setWrapper =
                (RangeSetWrapper<PositionImpl>) cursor.getIndividuallyDeletedMessagesSet();
        Field field = ManagedCursorImpl.class.getDeclaredField("rangeMarker");
        field.setAccessible(true);
        Map<Long, MLDataFormats.NestedPositionInfo> rangeMarker =
                (Map<Long, MLDataFormats.NestedPositionInfo>) field.get(cursor);
        //assertEquals(rangeMarker.size(), 1);
        assertTrue(setWrapper.getDirtyKeyRecorder().contains(3L));


        conf.setEnableLruCacheMaxUnackedRanges(true);
        // trigger persist
        restartBroker();

        consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName("my-sub").subscribe();
        topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        subscription = topic.getSubscription(sub);
        cursor = (ManagedCursorImpl) subscription.getCursor();
        assertTrue(cursor.getIndividuallyDeletedMessagesSet() instanceof RangeSetWrapper);
        setWrapper =
                (RangeSetWrapper<PositionImpl>) cursor.getIndividuallyDeletedMessagesSet();
        rangeMarker =
                (Map<Long, MLDataFormats.NestedPositionInfo>) field.get(cursor);
        //assertEquals(rangeMarker.size(), 1);
        assertEquals(cursor.getIndividuallyDeletedMessagesSet().size(), 4);
        assertEquals(cursor.getIndividuallyDeletedMessagesSet().toString(),
                "[(3:1..3:2],(3:3..3:4],(3:5..3:6],(3:7..3:8]]");
        assertFalse(setWrapper.getDirtyKeyRecorder().contains(3L));
        System.out.println("getIndividuallyDeletedMessagesSet2:" + rangeMarker);

        Set<Message<String>> restMessages = new HashSet<>();
        while (true) {
            Message<String> msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            assertTrue(restMessages.add(msg));
        }
        assertTrue(setWrapper.getDirtyKeyRecorder().contains(3L));

        assertEquals(restMessages.size(), msgNum - ackedMsg.size());
        for (Message<String> message : ackedMsg) {
            assertFalse(restMessages.contains(message));
        }
        consumer.close();
    }

    /**
     * Test validates if topic's dispatcher is stuck then broker can doscover and unblock it.
     *
     * @throws Exception
     */
    @Test
    public void testUnblockStuckSubscription() throws Exception {
        final String topicName = "persistent://prop/ns-abc/stuckSubscriptionTopic";
        final String sharedSubName = "shared";
        final String failoverSubName = "failOver";

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Shared).subscriptionName(sharedSubName).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Failover).subscriptionName(failoverSubName).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription sharedSub = topic.getSubscription(sharedSubName);
        PersistentSubscription failOverSub = topic.getSubscription(failoverSubName);

        PersistentDispatcherMultipleConsumers sharedDispatcher = (PersistentDispatcherMultipleConsumers) sharedSub
                .getDispatcher();
        PersistentDispatcherSingleActiveConsumer failOverDispatcher = (PersistentDispatcherSingleActiveConsumer) failOverSub
                .getDispatcher();

        // build backlog
        consumer1.close();
        consumer2.close();

        // block sub to read messages
        sharedDispatcher.havePendingRead = true;
        failOverDispatcher.havePendingRead = true;

        producer.newMessage().value("test").eventTime(5).send();
        producer.newMessage().value("test").eventTime(5).send();

        consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionType(SubscriptionType.Shared)
                .subscriptionName(sharedSubName).subscribe();
        consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionType(SubscriptionType.Failover)
                .subscriptionName(failoverSubName).subscribe();
        Message<String> msg = consumer1.receive(2, TimeUnit.SECONDS);
        assertNull(msg);
        msg = consumer2.receive(2, TimeUnit.SECONDS);
        assertNull(msg);

        // allow reads but dispatchers are still blocked
        sharedDispatcher.havePendingRead = false;
        failOverDispatcher.havePendingRead = false;

        // run task to unblock stuck dispatcher: first iteration sets the lastReadPosition and next iteration will
        // unblock the dispatcher read because read-position has not been moved since last iteration.
        sharedSub.checkAndUnblockIfStuck();
        failOverDispatcher.checkAndUnblockIfStuck();
        assertTrue(sharedSub.checkAndUnblockIfStuck());
        assertTrue(failOverDispatcher.checkAndUnblockIfStuck());

        msg = consumer1.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
        msg = consumer2.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
    }

    @Test
    public void testDeleteNamespaceInfiniteRetry() throws Exception {
        //init namespace
        final String myNamespace = "prop/ns" + UUID.randomUUID();
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testDeleteNamespaceInfiniteRetry";
        conf.setForceDeleteNamespaceAllowed(true);
        //init topic and policies
        pulsarClient.newProducer().topic(topic).create().close();
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 0);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == 0);

        PersistentTopic persistentTopic =
                spy((PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get());

        Policies policies = new Policies();
        policies.deleted = true;
        persistentTopic.onPoliciesUpdate(policies);
        verify(persistentTopic, times(0)).checkReplicationAndRetryOnFailure();

        policies.deleted = false;
        persistentTopic.onPoliciesUpdate(policies);
        verify(persistentTopic, times(1)).checkReplicationAndRetryOnFailure();
    }
}
