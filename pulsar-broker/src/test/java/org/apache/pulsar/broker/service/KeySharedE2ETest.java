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
import java.time.Duration;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate.Predicate4HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate.Predicate4HashRangeExclusiveStickyKeyConsumerSelector;

@Test(groups = "broker")
public class KeySharedE2ETest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.conf.setSubscriptionKeySharedUseConsistentHashing(false);
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

    protected void clearEventListener(TestConsumerStateEventListener eventListener){
        eventListener.keyPredicate = null;
        eventListener.trigCount.set(0);
    }

    @Test
    public void testConsumerEventsWithAutoHashRangeImpl() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/key-shared-topic_01-" + System.currentTimeMillis();
        final String subName = "sub_key_shared_01";
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared);
        int rangeSize = 0;
        // 1.One subscription.
        final TestConsumerStateEventListener listener1 = new TestConsumerStateEventListener("key_shared_listener_01");
        ConsumerBuilder<byte[]> consumerBuilder1 = consumerBuilder.clone().consumerName("key_shared_consumer_01")
                .consumerEventListener(listener1);
        Consumer<byte[]> consumer1 = consumerBuilder1.subscribe();
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> listener1.trigCount.get() == 1);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4HashRangeAutoSplitStickyKeyConsumerSelector);
        Predicate4HashRangeAutoSplitStickyKeyConsumerSelector predicate4ConsistentHashingStickyKeyConsumerSelector1 =
                (Predicate4HashRangeAutoSplitStickyKeyConsumerSelector) listener1.keyPredicate;
        rangeSize = predicate4ConsistentHashingStickyKeyConsumerSelector1.getRangeSize();
        Assert.assertTrue(predicate4ConsistentHashingStickyKeyConsumerSelector1.getRangeSize()
                == predicate4ConsistentHashingStickyKeyConsumerSelector1.getHighHash());
        Assert.assertEquals(predicate4ConsistentHashingStickyKeyConsumerSelector1.getLowHash(), -1);
        // 2.Two subscription.
        TestConsumerStateEventListener listener2 = new TestConsumerStateEventListener("key_shared_listener_02");
        ConsumerBuilder<byte[]> consumerBuilder2 = consumerBuilder.clone().consumerName("key_shared_consumer_02")
                .consumerEventListener(listener2);
        Consumer<byte[]> consumer2 = consumerBuilder2.subscribe();
        // assert listener2
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> listener2.trigCount.get() == 1);
        Assert.assertTrue(listener2.keyPredicate != null);
        Assert.assertTrue(listener2.keyPredicate instanceof Predicate4HashRangeAutoSplitStickyKeyConsumerSelector);
        Predicate4HashRangeAutoSplitStickyKeyConsumerSelector predicate4ConsistentHashingStickyKeyConsumerSelector2 =
                (Predicate4HashRangeAutoSplitStickyKeyConsumerSelector) listener2.keyPredicate;
        Assert.assertEquals(predicate4ConsistentHashingStickyKeyConsumerSelector2.getRangeSize(), rangeSize);
        Assert.assertEquals(predicate4ConsistentHashingStickyKeyConsumerSelector2.getHighHash(), rangeSize >> 1);
        Assert.assertEquals(predicate4ConsistentHashingStickyKeyConsumerSelector2.getLowHash(), -1);
        // assert listener1
        Assert.assertEquals(listener1.trigCount.get(), 2);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4HashRangeAutoSplitStickyKeyConsumerSelector);
        predicate4ConsistentHashingStickyKeyConsumerSelector1 =
                (Predicate4HashRangeAutoSplitStickyKeyConsumerSelector) listener1.keyPredicate;
        Assert.assertTrue(predicate4ConsistentHashingStickyKeyConsumerSelector1.getRangeSize()
                == predicate4ConsistentHashingStickyKeyConsumerSelector1.getHighHash());
        Assert.assertEquals(predicate4ConsistentHashingStickyKeyConsumerSelector1.getLowHash(), rangeSize >> 1);
        // 3. close one consumer.
        consumer2.close();
        // assert listener1
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> listener1.trigCount.get() == 3);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4HashRangeAutoSplitStickyKeyConsumerSelector);
        predicate4ConsistentHashingStickyKeyConsumerSelector1 =
                (Predicate4HashRangeAutoSplitStickyKeyConsumerSelector) listener1.keyPredicate;
        Assert.assertTrue(predicate4ConsistentHashingStickyKeyConsumerSelector1.getRangeSize()
                == predicate4ConsistentHashingStickyKeyConsumerSelector1.getHighHash());
        Assert.assertEquals(predicate4ConsistentHashingStickyKeyConsumerSelector1.getLowHash(), -1);
        // close sources.
        consumer1.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testConsumerEventsWithFixedHashRangeImpl() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/key-shared-topic_01-" + System.currentTimeMillis();
        final String subName = "sub_key_shared_01";
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared);
        int rangeSize = 0;
        // 1.One subscription.
        TestConsumerStateEventListener listener1 = new TestConsumerStateEventListener("key_shared_listener_01");
        ConsumerBuilder<byte[]> consumerBuilder1 = consumerBuilder.clone().consumerName("key_shared_consumer_01")
                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(0, 10)))
                .consumerEventListener(listener1);
        Consumer<byte[]> consumer1 = consumerBuilder1.subscribe();
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> listener1.trigCount.get() == 1);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4HashRangeExclusiveStickyKeyConsumerSelector);
        Predicate4HashRangeExclusiveStickyKeyConsumerSelector predicate1 =
                (Predicate4HashRangeExclusiveStickyKeyConsumerSelector) listener1.keyPredicate;
        rangeSize = predicate1.getRangeSize();
        NavigableMap<Integer, String> rangeMap1 = resolveRangeMap(predicate1);
        Assert.assertEquals(rangeMap1.size(), 2);
        Assert.assertEquals(rangeMap1.ceilingEntry(9).getValue(), StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(rangeMap1.floorEntry(9).getValue(), StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(rangeMap1.ceilingEntry(9).getKey(), Integer.valueOf(10));
        Assert.assertEquals(rangeMap1.floorEntry(9).getKey(), Integer.valueOf(0));
        // 2.Two subscription.
        TestConsumerStateEventListener listener2 = new TestConsumerStateEventListener("key_shared_listener_01");
        ConsumerBuilder<byte[]> consumerBuilder2 = consumerBuilder.clone().consumerName("key_shared_consumer_01")
                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(11, 20)))
                .consumerEventListener(listener2);
        Consumer<byte[]> consumer2 = consumerBuilder2.subscribe();
        // assert listener2
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> listener2.trigCount.get() == 1);
        Assert.assertTrue(listener2.keyPredicate != null);
        Assert.assertTrue(listener2.keyPredicate instanceof Predicate4HashRangeExclusiveStickyKeyConsumerSelector);
        Predicate4HashRangeExclusiveStickyKeyConsumerSelector predicate2 =
                (Predicate4HashRangeExclusiveStickyKeyConsumerSelector) listener2.keyPredicate;
        Assert.assertEquals(predicate2.getRangeSize(), rangeSize);
        NavigableMap<Integer, String> rangeMap2 = resolveRangeMap(predicate2);
        Assert.assertEquals(rangeMap2.size(), 4);
        Assert.assertEquals(rangeMap2.ceilingEntry(19).getValue(), StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(rangeMap2.floorEntry(19).getValue(), StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(rangeMap2.ceilingEntry(19).getKey(), Integer.valueOf(20));
        Assert.assertEquals(rangeMap2.floorEntry(19).getKey(), Integer.valueOf(11));
        // assert listener1
        Assert.assertEquals(listener1.trigCount.get(), 2);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4HashRangeExclusiveStickyKeyConsumerSelector);
        predicate1 = (Predicate4HashRangeExclusiveStickyKeyConsumerSelector) listener1.keyPredicate;
        Assert.assertEquals(predicate1.getRangeSize(), rangeSize);
        rangeMap1 = resolveRangeMap(predicate1);
        Assert.assertEquals(rangeMap1.size(), 4);
        Assert.assertEquals(rangeMap1.ceilingEntry(9).getValue(), StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(rangeMap1.floorEntry(9).getValue(), StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(rangeMap1.ceilingEntry(9).getKey(), Integer.valueOf(10));
        Assert.assertEquals(rangeMap1.floorEntry(9).getKey(), Integer.valueOf(0));
        // 3. close one consumer.
        consumer2.close();
        // assert listener1
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> listener1.trigCount.get() == 3);
        Assert.assertTrue(listener1.keyPredicate != null);
        Assert.assertTrue(listener1.keyPredicate instanceof Predicate4HashRangeExclusiveStickyKeyConsumerSelector);
        predicate1 = (Predicate4HashRangeExclusiveStickyKeyConsumerSelector) listener1.keyPredicate;
        Assert.assertEquals(predicate1.getRangeSize(), rangeSize);
        rangeMap1 = resolveRangeMap(predicate1);
        Assert.assertEquals(rangeMap1.size(), 2);
        Assert.assertEquals(rangeMap1.ceilingEntry(9).getValue(), StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(rangeMap1.floorEntry(9).getValue(), StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(rangeMap1.ceilingEntry(9).getKey(), Integer.valueOf(10));
        Assert.assertEquals(rangeMap1.floorEntry(9).getKey(), Integer.valueOf(0));
        // close sources.
        consumer1.close();
        admin.topics().delete(topicName);
    }

    private NavigableMap<Integer, String> resolveRangeMap(
            Predicate4HashRangeExclusiveStickyKeyConsumerSelector predicate){
        try {
            Field field = Predicate4HashRangeExclusiveStickyKeyConsumerSelector.class.getDeclaredField("rangeMap");
            field.setAccessible(true);
            return (NavigableMap<Integer, String>) field.get(predicate);
        } catch (Exception e){
            fail("should fail");
            return null;
        }
    }
}
