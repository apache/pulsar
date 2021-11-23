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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.fail;
import java.lang.reflect.Field;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate.Predicate4ConsistentHashingStickyKeyConsumerSelector;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class KeySharedEUseConsistentHashE2ETest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.conf.setSubscriptionKeySharedUseConsistentHashing(true);
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.conf.setSubscriptionKeySharedUseConsistentHashing(false);
        super.internalCleanup();
    }

    protected static final int CONSUMER_ADD_OR_REMOVE_WAIT_TIME = 100;

    protected static class TestConsumerStateEventListener implements ConsumerEventListener {

        String name = "";

        Predicate<String> keyPredicate;

        AtomicInteger trigCount = new AtomicInteger();

        public TestConsumerStateEventListener(String name) {
            this.name = name;
        }

        @Override
        public void becameActive(Consumer<?> consumer, int partitionId) {
        }

        @Override
        public void becameInactive(Consumer<?> consumer, int partitionId) {

        }

        @Override
        public void keySharedRuleChanged(Consumer<?> consumer, Predicate<String> keyPredicate) {
            this.keyPredicate = keyPredicate;
            trigCount.incrementAndGet();
        }
    }

    protected void clearEventListener(KeySharedE2ETest.TestConsumerStateEventListener eventListener){
        eventListener.keyPredicate = null;
        eventListener.trigCount.set(0);
    }

    @Test
    public void testConsumerEventsWithConsistentHashingImpl() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/key-shared-consistent-topic_01-"
                + System.currentTimeMillis();
        final String subName = "sub_key_shared_consistent_01";
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared);
        // 1.One subscription.
        TestConsumerStateEventListener listener1 =
                new TestConsumerStateEventListener("key_shared_consistent_listener_01");
        ConsumerBuilder<byte[]> consumerBuilder1 =
                consumerBuilder.clone().consumerName("key_shared_consistent_consumer_01")
                .consumerEventListener(listener1);
        Consumer<byte[]> consumer1 = consumerBuilder1.subscribe();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        Assert.assertEquals(listener1.trigCount.get(), 1);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4ConsistentHashingStickyKeyConsumerSelector);
        Predicate4ConsistentHashingStickyKeyConsumerSelector predicate1 =
                (Predicate4ConsistentHashingStickyKeyConsumerSelector) listener1.keyPredicate;
        NavigableMap<Integer, List<String>> rangeMap1 = resolveHashRing(predicate1);
        int rangeMapSizeFirst = rangeMap1.size();
        Assert.assertTrue(rangeMapSizeFirst > 0);
        long nodeCountFist = rangeMap1.values().stream().flatMap(list -> list.stream()).count();
        Assert.assertTrue(nodeCountFist > 0);
        // 2.Two subscription.
        TestConsumerStateEventListener listener2 = new TestConsumerStateEventListener("key_shared_listener_02");
        ConsumerBuilder<byte[]> consumerBuilder2 = consumerBuilder.clone().consumerName("key_shared_consumer_02")
                .consumerEventListener(listener2);
        Consumer<byte[]> consumer2 = consumerBuilder2.subscribe();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        // assert listener2
        Assert.assertEquals(listener2.trigCount.get(), 1);
        Assert.assertTrue(listener2.keyPredicate != null);
        Assert.assertTrue(listener2.keyPredicate instanceof Predicate4ConsistentHashingStickyKeyConsumerSelector);
        Predicate4ConsistentHashingStickyKeyConsumerSelector predicate2 =
                (Predicate4ConsistentHashingStickyKeyConsumerSelector) listener2.keyPredicate;
        NavigableMap<Integer, List<String>> rangeMap2 = resolveHashRing(predicate2);
        Assert.assertTrue(rangeMap2.size() >= rangeMapSizeFirst);
        long nodeCountSecond = rangeMap2.values().stream().flatMap(list -> list.stream()).count();
        Assert.assertTrue(nodeCountSecond > nodeCountFist);
        // assert listener1
        Assert.assertEquals(listener1.trigCount.get(), 2);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4ConsistentHashingStickyKeyConsumerSelector);
        predicate1 = (Predicate4ConsistentHashingStickyKeyConsumerSelector) listener1.keyPredicate;
        rangeMap1 = resolveHashRing(predicate1);
        Assert.assertTrue(rangeMap1.size() >= rangeMapSizeFirst);
        long nodeCount1 = rangeMap1.values().stream().flatMap(list -> list.stream()).count();
        Assert.assertTrue(nodeCount1 > nodeCountFist);
        // 3. Close one consumer
        consumer2.close();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        // assert listener1
        Assert.assertEquals(listener1.trigCount.get(), 3);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4ConsistentHashingStickyKeyConsumerSelector);
        predicate1 = (Predicate4ConsistentHashingStickyKeyConsumerSelector) listener1.keyPredicate;
        rangeMap1 = resolveHashRing(predicate1);
        Assert.assertEquals(rangeMap1.size(), rangeMapSizeFirst);
        Assert.assertEquals(rangeMap1.values().stream().flatMap(list -> list.stream()).count(), nodeCountFist);
        // close sources.
        consumer1.close();
        admin.topics().delete(topicName);
    }

    private NavigableMap<Integer, List<String>> resolveHashRing(
            Predicate4ConsistentHashingStickyKeyConsumerSelector predicate){
        try {
            Field field = Predicate4ConsistentHashingStickyKeyConsumerSelector.class.getDeclaredField("hashRing");
            field.setAccessible(true);
            return (NavigableMap<Integer, List<String>>) field.get(predicate);
        } catch (Exception e){
            fail("should fail");
            return null;
        }
    }
}
