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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Coverage for V5 scalable topics across a multi-broker cluster.
 *
 * <p>Each scalable topic has a single {@link org.apache.pulsar.broker.service.scalable.ScalableTopicController}
 * leader; the leader's brokerId is recorded in metadata. With three brokers and many topics
 * the leader role spreads naturally across the cluster, so tests need to cope with admin
 * operations and consumer sessions that target a non-owning broker. These tests assert that
 * the routing layers do their job:
 *
 * <ul>
 *   <li>controller leadership distributes across brokers given enough topics;</li>
 *   <li>{@code admin.scalableTopics()} mutating calls (split / merge) follow the HTTP 307
 *       redirect emitted by non-leader brokers and complete on the leader;</li>
 *   <li>V5 StreamConsumer attached through a non-owning broker still reaches the right
 *       controller via the lookup → controller-URL path and receives messages;</li>
 *   <li>V5 CheckpointConsumer with {@code consumerGroup(...)} (the only Checkpoint mode that
 *       runs through the controller) does the same.</li>
 * </ul>
 */
public class V5MultiBrokerScalableTopicTest extends V5MultiBrokerClientBaseTest {

    /**
     * Smoke: create a scalable topic, produce on broker 0's V5 client, consume on the last
     * broker's V5 client. Lookups across brokers must converge on the segment owners.
     */
    @Test
    public void testProduceConsumeAcrossBrokers() throws Exception {
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = v5Clients.get(0).newProducer(Schema.string())
                .topic(topic)
                .create();

        @Cleanup
        QueueConsumer<String> consumer = v5Clients.get(v5Clients.size() - 1)
                .newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("smoke-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int n = 30;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        Set<String> received = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missing message #" + i);
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(received, sent);
    }

    /**
     * Drive controller materialization through a different broker each iteration so the
     * leader-election race naturally lands on different brokers. The first broker to call
     * {@code getOrCreateController} for a topic wins (subsequent peers just observe the
     * existing leader); cycling the "first" broker spreads leadership across the cluster.
     */
    @Test
    public void testControllerLeadershipDistributesAcrossBrokers() throws Exception {
        int numTopics = brokers.size() * 4;
        Set<String> leaders = new HashSet<>();
        for (int i = 0; i < numTopics; i++) {
            String topic = newScalableTopic(1);
            int firstBroker = i % brokers.size();
            // Force this broker to materialize the controller first → it becomes leader.
            brokers.get(firstBroker).getBrokerService().getScalableTopicService()
                    .getOrCreateController(TopicName.get(topic))
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);
            String leader = findControllerLeader(topic);
            assertNotNull(leader, "controller leader must be elected for " + topic);
            leaders.add(leader);
        }
        assertEquals(leaders.size(), brokers.size(),
                "expected controller leadership to spread across every broker, got " + leaders);
    }

    /**
     * Splitting a scalable topic must succeed when the request hits a non-leader broker:
     * the admin layer redirects the call to the controller-leader broker via 307, the admin
     * client follows the redirect, and the metadata reflects the split.
     */
    @Test
    public void testSplitFromNonLeaderBrokerRedirectsToOwner() throws Exception {
        String topic = newScalableTopic(1);

        // Force the parent segment topic to load by producing a message — split's
        // terminate step requires the segment to exist on its owning broker.
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage().value("warm-up").send();

        int leaderIndex = findControllerLeaderIndex(topic);
        int nonLeaderIndex = (leaderIndex + 1) % brokers.size();

        long activeId = singleActiveSegmentId(topic);
        // Issue the split through a non-leader broker's admin. Without redirect this would
        // fail with a "not the leader" error; with redirect the leader applies the split.
        admins.get(nonLeaderIndex).scalableTopics().splitSegment(topic, activeId);

        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            var meta = admin.scalableTopics().getMetadata(topic);
            for (var seg : meta.getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 2, "split must produce 2 active children");
        });
    }

    /**
     * Same as {@link #testSplitFromNonLeaderBrokerRedirectsToOwner()}, but for merge:
     * prepare a topic with two adjacent active children (via a split), then merge them
     * through a non-leader broker's admin and assert the merge completed.
     */
    @Test
    public void testMergeFromNonLeaderBrokerRedirectsToOwner() throws Exception {
        String topic = newScalableTopic(1);
        // Warm-up — same reason as split: the parent segment topic must be loaded
        // on its owning broker before split/merge can terminate it.
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage().value("warm-up").send();

        long parentId = singleActiveSegmentId(topic);
        admin.scalableTopics().splitSegment(topic, parentId);
        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            for (var seg : admin.scalableTopics().getMetadata(topic).getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 2);
        });

        var meta = admin.scalableTopics().getMetadata(topic);
        long[] activeIds = new long[2];
        int idx = 0;
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                activeIds[idx++] = seg.getSegmentId();
            }
        }

        int leaderIndex = findControllerLeaderIndex(topic);
        int nonLeaderIndex = (leaderIndex + 1) % brokers.size();
        admins.get(nonLeaderIndex).scalableTopics()
                .mergeSegments(topic, activeIds[0], activeIds[1]);

        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            for (var seg : admin.scalableTopics().getMetadata(topic).getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 1, "merge must collapse to a single active segment");
        });
    }

    /**
     * A V5 StreamConsumer subscribed via a non-owning broker must still reach the controller
     * leader through the DAG-watch lookup → controller-URL path. Two consumers sharing the
     * subscription on different brokers should split segments via the controller and together
     * deliver every message exactly once.
     */
    @Test
    public void testStreamConsumerControllerCoordinationAcrossBrokers() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "cross-broker-stream";

        @Cleanup
        Producer<String> producer = v5Clients.get(0).newProducer(Schema.string())
                .topic(topic)
                .create();

        int leaderIndex = findControllerLeaderIndex(topic);
        int nonLeaderA = (leaderIndex + 1) % brokers.size();
        int nonLeaderB = (leaderIndex + 2) % brokers.size();

        @Cleanup
        StreamConsumer<String> a = v5Clients.get(nonLeaderA).newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("alice")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        StreamConsumer<String> b = v5Clients.get(nonLeaderB).newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("bob")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int n = 80;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        Set<String> received = ConcurrentHashMap.newKeySet();
        Set<String> aGot = ConcurrentHashMap.newKeySet();
        Set<String> bGot = ConcurrentHashMap.newKeySet();
        Thread ta = drainStream(a, received, aGot);
        Thread tb = drainStream(b, received, bGot);
        ta.join();
        tb.join();

        assertEquals(received, sent, "every message must be delivered exactly once across the group");
        Set<String> overlap = new HashSet<>(aGot);
        overlap.retainAll(bGot);
        assertTrue(overlap.isEmpty(), "no message should be delivered to both consumers, overlap=" + overlap);
        assertTrue(!aGot.isEmpty() && !bGot.isEmpty(),
                "controller must split segments across both consumers (a=" + aGot.size()
                        + " b=" + bGot.size() + ")");
    }

    /**
     * V5 CheckpointConsumer with {@code consumerGroup(...)} routes its session through the
     * topic controller (same path as StreamConsumer). The same cross-broker test as above:
     * two checkpoint consumers in a group, attached via non-owning brokers, must receive
     * disjoint subsets that together cover every produced message.
     */
    @Test
    public void testCheckpointConsumerControllerCoordinationAcrossBrokers() throws Exception {
        String topic = newScalableTopic(4);
        String group = "cross-broker-checkpoint-group";

        @Cleanup
        Producer<String> producer = v5Clients.get(0).newProducer(Schema.string())
                .topic(topic)
                .create();

        int leaderIndex = findControllerLeaderIndex(topic);
        int nonLeaderA = (leaderIndex + 1) % brokers.size();
        int nonLeaderB = (leaderIndex + 2) % brokers.size();

        @Cleanup
        CheckpointConsumer<String> a = v5Clients.get(nonLeaderA)
                .newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();
        @Cleanup
        CheckpointConsumer<String> b = v5Clients.get(nonLeaderB)
                .newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();

        int n = 80;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        Set<String> received = ConcurrentHashMap.newKeySet();
        Set<String> aGot = ConcurrentHashMap.newKeySet();
        Set<String> bGot = ConcurrentHashMap.newKeySet();
        Thread ta = drainCheckpoint(a, received, aGot);
        Thread tb = drainCheckpoint(b, received, bGot);
        ta.join();
        tb.join();

        assertEquals(received, sent, "every message must be delivered exactly once across the group");
        Set<String> overlap = new HashSet<>(aGot);
        overlap.retainAll(bGot);
        assertTrue(overlap.isEmpty(),
                "no message should be delivered to both checkpoint consumers, overlap=" + overlap);
        assertTrue(!aGot.isEmpty() && !bGot.isEmpty(),
                "controller must split segments across both consumers (a=" + aGot.size()
                        + " b=" + bGot.size() + ")");
    }

    // --- Helpers ---

    /**
     * Returns the brokerId of the controller leader for {@code topic}. Forces the controller
     * to materialize on every broker (so leader election runs), then waits until every
     * broker's metadata store reflects the elected leader. The latter wait is what makes the
     * subsequent V5 subscribe deterministic: the DAG-watch lookup from any broker reads the
     * controller znode via its own metadata store, and we need that read to return the
     * leader URL — not the empty fallback that pushes the client onto a non-leader broker.
     */
    private String findControllerLeader(String topic) throws Exception {
        TopicName tn = TopicName.get(topic);
        // Step 1: force controller materialization + leader election on every broker.
        for (PulsarService broker : brokers) {
            broker.getBrokerService().getScalableTopicService().getOrCreateController(tn)
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        // Step 2: wait until each broker's metadata store sees the controller-lock znode.
        // Without this, a lookup against a follower can return an empty controller URL —
        // the watch hasn't propagated yet — and the client subscribes to the wrong broker.
        Awaitility.await().untilAsserted(() -> {
            for (PulsarService broker : brokers) {
                var resources = broker.getPulsarResources().getScalableTopicResources();
                var optValue = resources.getStore().get(resources.controllerLockPath(tn))
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertTrue(optValue.isPresent(),
                        "broker " + broker.getBrokerId()
                                + " must see controller lock for " + topic);
            }
        });
        var controller = brokers.get(0).getBrokerService().getScalableTopicService()
                .getOrCreateController(tn).get();
        return controller.getLeaderBrokerId().get().orElseThrow();
    }

    private int findControllerLeaderIndex(String topic) throws Exception {
        String leaderBrokerId = findControllerLeader(topic);
        for (int i = 0; i < brokers.size(); i++) {
            if (brokers.get(i).getBrokerId().equals(leaderBrokerId)) {
                return i;
            }
        }
        throw new AssertionError("controller leader '" + leaderBrokerId
                + "' does not match any broker in cluster");
    }

    /**
     * Returns the segment id of the (single) active segment of {@code topic}. Convenience
     * for tests that work on a freshly-created scalable topic with one initial segment.
     */
    private long singleActiveSegmentId(String topic) throws Exception {
        var meta = admin.scalableTopics().getMetadata(topic);
        long active = -1;
        int count = 0;
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                active = seg.getSegmentId();
                count++;
            }
        }
        assertEquals(count, 1, "expected exactly one active segment");
        assertNotEquals(active, -1L);
        return active;
    }

    private Thread drainStream(StreamConsumer<String> consumer, Set<String> all, Set<String> mine) {
        Thread t = new Thread(() -> {
            try {
                MessageId last = null;
                while (true) {
                    Message<String> msg = consumer.receive(Duration.ofSeconds(1));
                    if (msg == null) {
                        if (last != null) {
                            consumer.acknowledgeCumulative(last);
                        }
                        return;
                    }
                    all.add(msg.value());
                    mine.add(msg.value());
                    last = msg.id();
                }
            } catch (Exception ignored) {
            }
        }, "stream-consumer-drainer");
        t.start();
        return t;
    }

    private Thread drainCheckpoint(CheckpointConsumer<String> consumer,
                                   Set<String> all, Set<String> mine) {
        Thread t = new Thread(() -> {
            try {
                while (true) {
                    Message<String> msg = consumer.receive(Duration.ofSeconds(1));
                    if (msg == null) {
                        return;
                    }
                    all.add(msg.value());
                    mine.add(msg.value());
                }
            } catch (Exception ignored) {
            }
        }, "checkpoint-consumer-drainer");
        t.start();
        return t;
    }
}
