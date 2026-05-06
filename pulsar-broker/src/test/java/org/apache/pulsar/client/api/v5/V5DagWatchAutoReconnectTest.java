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
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Coverage for {@code DagWatchClient}'s auto-reconnect path. The DAG watch holds
 * the producer / consumer's view of the scalable topic layout — without
 * reconnect, a transient broker disconnect silently strands the client on stale
 * layout and never delivers another split / merge update.
 *
 * <p>These tests force-close the watch's underlying channel and assert that
 * producers and consumers continue to operate end-to-end without application
 * intervention.
 */
public class V5DagWatchAutoReconnectTest extends V5ClientBaseTest {

    /**
     * Producer's DAG watch channel is force-closed mid-life. Sends made after the
     * close must still succeed: the cached layout keeps existing segments
     * reachable, and the reconnect re-establishes the watch so subsequent layout
     * changes would still be observed.
     */
    @Test
    public void testProducerSurvivesDagWatchConnectionDrop() throws Exception {
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("dag-reconnect-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int firstN = 10;
        Set<String> firstSent = new HashSet<>();
        for (int i = 0; i < firstN; i++) {
            String v = "first-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            firstSent.add(v);
        }
        assertEquals(drain(consumer, firstN), firstSent,
                "first batch must arrive before the disconnect");

        // Force-close the DAG watch channel. The cnx layer fires connectionClosed()
        // on the DagWatchClient, which schedules a reconnect.
        forceCloseDagWatchOnProducer(producer);

        // Send a second batch immediately. Existing segments are still reachable
        // through the per-segment v4 producers (their own connections are unaffected),
        // so this proves the producer keeps working through the reconnect window.
        int secondN = 10;
        Set<String> secondSent = new HashSet<>();
        for (int i = 0; i < secondN; i++) {
            String v = "second-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            secondSent.add(v);
        }
        assertEquals(drain(consumer, secondN), secondSent,
                "producer must keep sending after DAG watch reconnect kicks in");
    }

    /**
     * After a force-close, the DAG watch must observe a fresh broker connection
     * (i.e., its internal {@code cnx} field is re-populated). Asserts the
     * reconnect path actually fires rather than silently staying disconnected.
     */
    @Test
    public void testDagWatchReattachesAfterDisconnect() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        Object dagWatch = getDagWatchOnProducer(producer);
        Object originalCnx = readField(dagWatch, "cnx");
        assertNotNull(originalCnx, "DAG watch must have an initial connection");

        forceCloseDagWatchOnProducer(producer);

        // Wait for the reconnect path to land a fresh ClientCnx on the DagWatchClient.
        // Backoff starts at 100ms; allow a generous window for CI.
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .until(() -> {
                    Object current = readField(dagWatch, "cnx");
                    return current != null && current != originalCnx;
                });
    }

    /**
     * A consumer's DAG watch channel is force-closed mid-life. Like the producer
     * test, this asserts the consumer continues to deliver messages produced
     * after the disconnect.
     */
    @Test
    public void testConsumerSurvivesDagWatchConnectionDrop() throws Exception {
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("dag-reconnect-consumer-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int firstN = 10;
        for (int i = 0; i < firstN; i++) {
            producer.newMessage().key("k-" + i).value("first-" + i).send();
        }
        assertEquals(drain(consumer, firstN).size(), firstN,
                "first batch must arrive before disconnect");

        forceCloseDagWatchOnConsumer(consumer);

        int secondN = 10;
        for (int i = 0; i < secondN; i++) {
            producer.newMessage().key("k-" + i).value("second-" + i).send();
        }
        Set<String> got = drain(consumer, secondN);
        assertEquals(got.size(), secondN,
                "consumer must keep receiving after DAG watch reconnect kicks in");
    }

    // --- Helpers ---

    private Set<String> drain(QueueConsumer<String> consumer, int expected) throws Exception {
        Set<String> received = new HashSet<>();
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < expected && System.currentTimeMillis() < deadline) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
                consumer.acknowledge(msg.id());
            }
        }
        return received;
    }

    private static Object getDagWatchOnProducer(Producer<?> producer) throws Exception {
        Field f = producer.getClass().getDeclaredField("dagWatch");
        f.setAccessible(true);
        Object watch = f.get(producer);
        assertNotNull(watch, "expected dagWatch on producer");
        return watch;
    }

    private static void forceCloseDagWatchOnProducer(Producer<?> producer) throws Exception {
        Object watch = getDagWatchOnProducer(producer);
        Method m = watch.getClass().getDeclaredMethod("forceCloseConnectionForTesting");
        m.setAccessible(true);
        m.invoke(watch);
    }

    private static void forceCloseDagWatchOnConsumer(QueueConsumer<?> consumer) throws Exception {
        Field f = consumer.getClass().getDeclaredField("dagWatch");
        f.setAccessible(true);
        Object watch = f.get(consumer);
        assertNotNull(watch, "expected dagWatch on consumer");
        Method m = watch.getClass().getDeclaredMethod("forceCloseConnectionForTesting");
        m.setAccessible(true);
        m.invoke(watch);
    }

    private static Object readField(Object target, String name) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }
}
