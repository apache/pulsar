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
import static org.testng.Assert.assertNotNull;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for automatic reconnect on the scalable consumer's controller session.
 *
 * <p>A V5 StreamConsumer keeps a long-lived subscription against the topic
 * controller — that's how it learns about assignment changes (split / merge /
 * peer join / peer leave). When the underlying connection drops mid-life
 * (broker bounce, network blip, container restart) the per-segment v4 consumers
 * have their own reconnect logic, but the controller session needs its own.
 *
 * <p>These tests force-close the controller channel and assert:
 * <ul>
 *   <li>the consumer continues to deliver messages without the application
 *       having to do anything; and</li>
 *   <li>within the controller's grace window the assignment is preserved, so a
 *       second consumer that didn't drop sees no reshuffling.</li>
 * </ul>
 */
public class V5StreamConsumerAutoReconnectTest extends V5ClientBaseTest {

    /**
     * Force-close the controller channel underneath a single stream consumer and
     * verify it continues to receive messages produced after the disconnect. The
     * v5 layer must reattach to the controller and resume receiving without any
     * application-visible interruption.
     */
    @Test
    public void testStreamConsumerSurvivesConnectionDrop() throws Exception {
        String topic = newScalableTopic(2);
        String subscription = "auto-reconnect-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("solo")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Pre-disconnect batch: prove the consumer is healthy before we sever.
        int firstN = 20;
        Set<String> firstSent = new HashSet<>();
        for (int i = 0; i < firstN; i++) {
            String v = "first-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            firstSent.add(v);
        }
        Set<String> firstReceived = drain(consumer, firstN);
        assertEquals(firstReceived, firstSent, "first batch must arrive in full before disconnect");

        // Sever the controller channel. The cnx layer fires connectionClosed() on
        // the scalable session, which kicks off the reconnect path with backoff.
        forceCloseControllerChannel(consumer);

        // Post-disconnect batch: produced after the channel close. The consumer
        // must auto-reconnect to the controller and resume serving its segments.
        int secondN = 20;
        Set<String> secondSent = new HashSet<>();
        for (int i = 0; i < secondN; i++) {
            String v = "second-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            secondSent.add(v);
        }
        Set<String> secondReceived = drain(consumer, secondN);
        assertEquals(secondReceived, secondSent,
                "consumer must resume receiving after auto-reconnect");
    }

    /**
     * Two stream consumers share the topic, then alice's controller channel is
     * forcibly closed. Within the grace window (2s in the test cluster) alice's
     * auto-reconnect should re-attach to the existing session and the same
     * assignment, so bob's per-key share is unchanged across batches.
     */
    @Test
    public void testReconnectWithinGracePreservesAssignment() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "auto-reconnect-grace-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        @Cleanup
        StreamConsumer<String> alice = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("alice")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        StreamConsumer<String> bob = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("bob")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Baseline batch: capture each consumer's per-key share before the drop.
        int batchN = 60;
        Set<String> firstSent = new HashSet<>();
        for (int i = 0; i < batchN; i++) {
            String v = "first-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            firstSent.add(v);
        }
        Set<String> aliceFirst = new HashSet<>();
        Set<String> bobFirst = new HashSet<>();
        Set<String> received1 = new HashSet<>();
        drainBoth(alice, bob, batchN, received1, aliceFirst, bobFirst);
        assertEquals(received1, firstSent, "first batch must be delivered exactly once");

        // Sever alice's controller channel. The reconnect must land well within
        // the 2s grace window — the controller still has alice's session and
        // re-attaches the new connection to the existing assignment.
        forceCloseControllerChannel(alice);

        int secondN = 60;
        Set<String> secondSent = new HashSet<>();
        for (int i = 0; i < secondN; i++) {
            String v = "second-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            secondSent.add(v);
        }
        Set<String> aliceSecond = new HashSet<>();
        Set<String> bobSecond = new HashSet<>();
        Set<String> received2 = new HashSet<>();
        drainBoth(alice, bob, secondN, received2, aliceSecond, bobSecond);
        assertEquals(received2, secondSent, "second batch must be delivered exactly once");

        // Same key set, same routing → each consumer's per-key share must be
        // identical across batches (modulo the prefix). That holds only if alice
        // re-attached to her existing session — i.e. the reconnect landed within
        // grace and no rebalance happened.
        assertEquals(stripPrefix(aliceSecond, "second-"), stripPrefix(aliceFirst, "first-"),
                "alice must keep her segments after auto-reconnect within grace");
        assertEquals(stripPrefix(bobSecond, "second-"), stripPrefix(bobFirst, "first-"),
                "bob must be unaffected by alice's reconnect");
    }

    // --- Helpers ---

    /**
     * Drain at most {@code expected} messages from {@code consumer}, capping at a
     * generous deadline so a regression doesn't hang the suite.
     */
    private Set<String> drain(StreamConsumer<String> consumer, int expected) throws Exception {
        Set<String> received = new HashSet<>();
        MessageId last = null;
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < expected && System.currentTimeMillis() < deadline) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
                last = msg.id();
            }
        }
        if (last != null) {
            consumer.acknowledgeCumulative(last);
        }
        return received;
    }

    /**
     * Drain a known number of messages across two consumers, populating each
     * consumer's per-message set. Stops once {@code allReceived} reaches
     * {@code expected} or the deadline fires.
     */
    private void drainBoth(StreamConsumer<String> a, StreamConsumer<String> b,
                           int expected, Set<String> allReceived,
                           Set<String> aGot, Set<String> bGot) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000L;
        MessageId aLast = null;
        MessageId bLast = null;
        while (allReceived.size() < expected && System.currentTimeMillis() < deadline) {
            Message<String> ma = a.receive(Duration.ofMillis(200));
            if (ma != null) {
                allReceived.add(ma.value());
                aGot.add(ma.value());
                aLast = ma.id();
            }
            Message<String> mb = b.receive(Duration.ofMillis(200));
            if (mb != null) {
                allReceived.add(mb.value());
                bGot.add(mb.value());
                bLast = mb.id();
            }
        }
        if (aLast != null) {
            a.acknowledgeCumulative(aLast);
        }
        if (bLast != null) {
            b.acknowledgeCumulative(bLast);
        }
    }

    private static Set<String> stripPrefix(Set<String> values, String prefix) {
        Set<String> out = new HashSet<>(values.size());
        for (String v : values) {
            if (v.startsWith(prefix)) {
                out.add(v.substring(prefix.length()));
            }
        }
        return out;
    }

    /**
     * Force-close the controller channel underneath a stream consumer. Reaches
     * into the v5 internals via reflection because the {@code session} field on
     * {@code ScalableStreamConsumer} and the test hook on
     * {@code ScalableConsumerClient} are package-private.
     */
    private static void forceCloseControllerChannel(StreamConsumer<?> consumer) throws Exception {
        Field sessionField = consumer.getClass().getDeclaredField("session");
        sessionField.setAccessible(true);
        Object session = sessionField.get(consumer);
        assertNotNull(session, "expected session on stream consumer");
        Method m = session.getClass().getDeclaredMethod("forceCloseConnectionForTesting");
        m.setAccessible(true);
        m.invoke(session);
    }
}
