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
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.TopicPolicyListener;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.compaction.CompactionServiceFactory;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class OrphanPersistentTopicTest extends ProducerConsumerBase {

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

    @Test
    public void testNoOrphanTopicAfterCreateTimeout() throws Exception {
        // Make the topic loading timeout faster.
        int topicLoadTimeoutSeconds = 2;
        long originalTopicLoadTimeoutSeconds = pulsar.getConfig().getTopicLoadTimeoutSeconds();
        pulsar.getConfig().setTopicLoadTimeoutSeconds(2);

        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String mlPath = BrokerService.MANAGED_LEDGER_PATH_ZNODE + "/" + TopicName.get(tpName).getPersistenceNamingEncoding();

        // Make topic load timeout 5 times.
        AtomicInteger timeoutCounter = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            mockZooKeeper.delay(topicLoadTimeoutSeconds * 2 * 1000, (op, path) -> {
                if (mlPath.equals(path)) {
                    log.info("Topic load timeout: " + timeoutCounter.incrementAndGet());
                    return true;
                }
                return false;
            });
        }

        // Load topic.
        CompletableFuture<Consumer<byte[]>> consumer = pulsarClient.newConsumer()
                .topic(tpName)
                .subscriptionName("my-sub")
                .subscribeAsync();

        // After create timeout 5 times, the topic will be created successful.
        Awaitility.await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> future = pulsar.getBrokerService().getTopic(tpName, false);
            assertTrue(future.isDone());
            Optional<Topic> optional = future.get();
            assertTrue(optional.isPresent());
        });

        // Assert only one PersistentTopic was not closed.
        TopicPoliciesService topicPoliciesService = pulsar.getTopicPoliciesService();
        Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listeners =
                WhiteboxImpl.getInternalState(topicPoliciesService, "listeners");
        assertEquals(listeners.get(TopicName.get(tpName)).size(), 1);

        // cleanup.
        consumer.join().close();
        admin.topics().delete(tpName, false);
        pulsar.getConfig().setTopicLoadTimeoutSeconds(originalTopicLoadTimeoutSeconds);
    }

    @Test
    public void testNoOrphanTopicIfInitFailed() throws Exception {
        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(tpName);

        // Load topic.
        Consumer consumer = pulsarClient.newConsumer()
                .topic(tpName)
                .subscriptionName("my-sub")
                .subscribe();

        // Make the method `PersitentTopic.initialize` fail.
        Field fieldCompactionServiceFactory = PulsarService.class.getDeclaredField("compactionServiceFactory");
        fieldCompactionServiceFactory.setAccessible(true);
        CompactionServiceFactory compactionServiceFactory =
                (CompactionServiceFactory) fieldCompactionServiceFactory.get(pulsar);
        fieldCompactionServiceFactory.set(pulsar, null);
        admin.topics().unload(tpName);

        // Wait for failed to create topic for several times.
        Thread.sleep(5 * 1000);

        // Remove the injected error, the topic will be created successful.
        fieldCompactionServiceFactory.set(pulsar, compactionServiceFactory);
        // We do not know the next time of consumer reconnection, so wait for 2 minutes to avoid flaky. It will be
        // very fast in normal.
        Awaitility.await().ignoreExceptions().atMost(120, TimeUnit.SECONDS).untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> future = pulsar.getBrokerService().getTopic(tpName, false);
            assertTrue(future.isDone());
            Optional<Topic> optional = future.get();
            assertTrue(optional.isPresent());
        });

        // Assert only one PersistentTopic was not closed.
        TopicPoliciesService topicPoliciesService = pulsar.getTopicPoliciesService();
        Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listeners =
                WhiteboxImpl.getInternalState(topicPoliciesService, "listeners");
        assertEquals(listeners.get(TopicName.get(tpName)).size(), 1);

        // cleanup.
        consumer.close();
        admin.topics().delete(tpName, false);
    }
}
