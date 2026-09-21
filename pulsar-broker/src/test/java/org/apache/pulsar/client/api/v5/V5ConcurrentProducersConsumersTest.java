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
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Sustained-throughput coverage: many producers + many QueueConsumers running
 * concurrently against the same multi-segment topic + subscription. Asserts no message
 * is dropped or duplicated under load, and that every consumer pulls its share.
 */
public class V5ConcurrentProducersConsumersTest extends V5ClientBaseTest {

    @Test
    public void testManyProducersAndConsumersConcurrent() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "concurrent-sub";

        int numProducers = 4;
        int numConsumers = 3;
        int messagesPerProducer = 100;
        int totalMessages = numProducers * messagesPerProducer;

        // Subscribe consumers up front so the subscription cursor is anchored at the
        // start before producers begin.
        List<QueueConsumer<String>> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            consumers.add(v5Client.newQueueConsumer(Schema.string())
                    .topic(topic)
                    .subscriptionName(subscription)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                    .subscribe());
        }

        // Per-consumer received sets so we can verify load distribution and disjointness.
        List<Set<String>> perConsumer = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            perConsumer.add(ConcurrentHashMap.newKeySet());
        }
        Set<String> received = ConcurrentHashMap.newKeySet();

        // Drainer threads: each consumer pulls until it sees no message for a quiet
        // window. They run for the full duration of the test.
        List<Thread> drainers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                QueueConsumer<String> c = consumers.get(idx);
                Set<String> mine = perConsumer.get(idx);
                try {
                    while (true) {
                        Message<String> msg = c.receive(Duration.ofSeconds(1));
                        if (msg == null) {
                            // No more messages for at least 1s — exit. Producers are
                            // expected to finish well before this idle window.
                            return;
                        }
                        received.add(msg.value());
                        mine.add(msg.value());
                        c.acknowledge(msg.id());
                    }
                } catch (Exception ignored) {
                }
            }, "drainer-" + i);
            t.start();
            drainers.add(t);
        }

        // Producer threads: each spins up its own V5 producer and sends a slice.
        Set<String> sent = ConcurrentHashMap.newKeySet();
        List<Thread> producerThreads = new ArrayList<>();
        for (int p = 0; p < numProducers; p++) {
            final int producerIdx = p;
            Thread t = new Thread(() -> {
                try (Producer<String> producer = v5Client.newProducer(Schema.string())
                        .topic(topic)
                        .create()) {
                    for (int i = 0; i < messagesPerProducer; i++) {
                        String v = "p" + producerIdx + "-m" + i;
                        producer.newMessage().key("k-" + producerIdx + "-" + i).value(v).send();
                        sent.add(v);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "producer-" + p);
            t.start();
            producerThreads.add(t);
        }

        for (Thread t : producerThreads) {
            t.join();
        }
        assertEquals(sent.size(), totalMessages, "every producer must complete its slice");

        // Wait for drainers to finish (they exit on a 1s idle window once all messages
        // are consumed). Cap the wait so a regression doesn't hang the suite.
        for (Thread t : drainers) {
            t.join(60_000L);
        }

        for (var c : consumers) {
            c.close();
        }

        assertEquals(received, sent, "every produced message must be consumed exactly once");

        // No duplicates across consumers.
        for (int i = 0; i < numConsumers; i++) {
            for (int j = i + 1; j < numConsumers; j++) {
                Set<String> overlap = new HashSet<>(perConsumer.get(i));
                overlap.retainAll(perConsumer.get(j));
                assertTrue(overlap.isEmpty(),
                        "consumers " + i + "/" + j + " must not overlap, got " + overlap.size()
                                + " duplicates");
            }
        }

        // Load distribution: every consumer must have pulled a non-trivial share.
        for (int i = 0; i < numConsumers; i++) {
            assertTrue(!perConsumer.get(i).isEmpty(),
                    "consumer #" + i + " must receive at least one message");
        }
    }
}
