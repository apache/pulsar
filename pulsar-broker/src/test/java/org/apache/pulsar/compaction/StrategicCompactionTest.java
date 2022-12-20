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
package org.apache.pulsar.compaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class StrategicCompactionTest extends CompactionTest {
    private TopicCompactionStrategy strategy;
    private StrategicTwoPhaseCompactor compactor;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.setup();
        compactor = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler, 1);
        strategy = new TopicCompactionStrategyTest.DummyTopicCompactionStrategy();
    }

    @Override
    protected long compact(String topic) throws ExecutionException, InterruptedException {
        return (long) compactor.compact(topic, strategy).get();
    }

    @Override
    protected long compact(String topic, CryptoKeyReader cryptoKeyReader)
            throws ExecutionException, InterruptedException {
        return (long) compactor.compact(topic, strategy, cryptoKeyReader).get();
    }

    @Override
    protected TwoPhaseCompactor getCompactor() {
        return compactor;
    }


    @Test
    public void testNumericOrderCompaction() throws Exception {

        strategy = new NumericOrderCompactionStrategy();

        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 50;
        final int maxKeys = 5;

        Producer<Integer> producer = pulsarClient.newProducer(strategy.getSchema())
                .topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Map<String, Integer> expected = new HashMap<>();
        List<Pair<String, Integer>> all = new ArrayList<>();
        Random r = new Random(0);

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            int seed = r.nextInt(j + 1);
            Integer cur = seed < j / 5 ? null : seed;
            producer.newMessage().key(key).value(cur).send();
            Integer prev = expected.get(key);
            if (!strategy.shouldKeepLeft(prev, cur)) {
                if (cur == null) {
                    expected.remove(key);
                } else {
                    expected.put(key, cur);
                }
            }
            all.add(Pair.of(key, cur));
        }

        compact(topic);

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topic, false);
        // Compacted topic ledger should have same number of entry equals to number of unique key.
        Assert.assertEquals(expected.size(), internalStats.compactedLedger.entries);
        Assert.assertTrue(internalStats.compactedLedger.ledgerId > -1);
        Assert.assertFalse(internalStats.compactedLedger.offloaded);

        Map<String, Integer> expectedCopy = new HashMap<>(expected);
        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<Integer> consumer = pulsarClient.newConsumer(strategy.getSchema()).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            while (!expected.isEmpty()) {
                Message<Integer> m = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertEquals(m.getValue(), expected.remove(m.getKey()), m.getKey());
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Consumer<Integer> consumer = pulsarClient.newConsumer(strategy.getSchema()).topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            while (true) {
                Message<Integer> m = consumer.receive(2, TimeUnit.SECONDS);
                Pair<String, Integer> expectedMessage = all.remove(0);
                Assert.assertEquals(m.getKey(), expectedMessage.getLeft());
                Assert.assertEquals(m.getValue(), expectedMessage.getRight());
                if (all.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(all.isEmpty());
        }

        TableView<Integer> tableView = pulsar.getClient().newTableViewBuilder(strategy.getSchema())
                .topic(topic)
                .loadConf(Map.of(
                        "topicCompactionStrategyClassName", strategy.getClass().getCanonicalName()))
                .create();
        Assert.assertEquals(tableView.entrySet(), expectedCopy.entrySet());
    }


}
