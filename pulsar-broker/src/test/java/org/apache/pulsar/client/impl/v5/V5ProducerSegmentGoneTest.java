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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A send that finds its target segment gone waits for the next layout and retries. While it waits,
 * the per-segment producer that is gone must stay where it is: dropping it would have every send
 * routed to the segment in the meantime create a new v4 producer on a terminated topic, thousands
 * of them a second under load. Once the producer or its client is closing, no layout is coming and
 * the sends fail instead of waiting for one.
 */
public class V5ProducerSegmentGoneTest extends V5ClientBaseTest {

    /** Sends per burst; a burst is followed by a millisecond pause, a pace the broker keeps up with. */
    private static final int BURST = 20;

    /** The sends must keep flowing while a segment is sealed, so the limit must not block the sender. */
    private PulsarClient newClient() throws Exception {
        return track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .memoryLimit(MemorySize.ofBytes(256 * 1024 * 1024))
                .build());
    }

    private ScalableTopicProducer<byte[]> newProducer(PulsarClient client, String topic) throws Exception {
        return (ScalableTopicProducer<byte[]>) track(client.newProducer(Schema.bytes())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create());
    }

    /** The sends a {@link #startSender sender} thread has issued and what became of them. */
    private static final class Sends {
        final List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        final AtomicInteger issued = new AtomicInteger();
        final AtomicInteger completed = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();
        final Set<Long> segmentsAcked = ConcurrentHashMap.newKeySet();
        final AtomicBoolean pause = new AtomicBoolean();
        final AtomicBoolean stop = new AtomicBoolean();

        boolean drained() {
            return completed.get() == issued.get();
        }
    }

    /** Send paced bursts, pausing on request, until told to stop. */
    private static Thread startSender(Producer<byte[]> producer, Sends sends) {
        byte[] payload = new byte[1024];
        Thread sender = new Thread(() -> {
            while (!sends.stop.get()) {
                if (!sends.pause.get()) {
                    for (int i = 0; i < BURST; i++) {
                        CompletableFuture<MessageId> future = producer.async().newMessage().value(payload).send();
                        future.whenComplete((id, ex) -> {
                            if (ex == null) {
                                sends.segmentsAcked.add(((MessageIdV5) id).segmentId());
                            } else {
                                sends.failed.incrementAndGet();
                            }
                            sends.completed.incrementAndGet();
                        });
                        sends.futures.add(future);
                        sends.issued.incrementAndGet();
                    }
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }, "sender");
        sender.start();
        return sender;
    }

    /** Stop the sender whatever state a test left it in, so that a failing test does not leak it. */
    private static void stopSender(Thread sender, Sends sends) throws InterruptedException {
        sends.stop.set(true);
        sender.join(TimeUnit.SECONDS.toMillis(10));
        if (sender.isAlive()) {
            // Blocked in a send: on the memory limit, or on whatever the failed test left hanging.
            sender.interrupt();
            sender.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    private static CompletableFuture<Void> allSettled(Sends sends) {
        return CompletableFuture.allOf(sends.futures.toArray(new CompletableFuture[0]))
                .exceptionally(__ -> null);
    }

    private long singleActiveSegmentId(String topic) throws Exception {
        for (var segment : admin.scalableTopics().getMetadata(topic).getSegments().values()) {
            if (segment.isActive()) {
                return segment.getSegmentId();
            }
        }
        throw new AssertionError("no active segment for " + topic);
    }

    private String segmentTopic(String topic, long segmentId) throws Exception {
        var segment = admin.scalableTopics().getMetadata(topic).getSegments().get(segmentId);
        HashRange range = HashRange.of(segment.getHashRange().getStart(), segment.getHashRange().getEnd());
        return SegmentTopicName.fromParent(TopicName.get(topic), range, segmentId).toString();
    }

    @Test
    public void aSealedSegmentGetsNoNewProducerForTheSendsThatFindItGone() throws Exception {
        PulsarClient client = newClient();
        String topic = newScalableTopic(1);
        long segmentId = singleActiveSegmentId(topic);
        ScalableTopicProducer<byte[]> producer = newProducer(client, topic);

        Sends sends = new Sends();
        Thread sender = startSender(producer, sends);
        try {
            Awaitility.await().until(() -> sends.segmentsAcked.contains(segmentId));
            // Terminate the segment topic underneath the layout: the state a split leaves the
            // producer in until the DAG watch delivers the new layout, held here until the sends'
            // retry budget runs out. Terminated while quiet, so that no write fails on the closing
            // ledger and fences the topic instead; then the sends flow again, and on past the first
            // failure.
            sends.pause.set(true);
            Awaitility.await().until(sends::drained);
            admin.scalableTopics().terminateSegment(segmentTopic(topic, segmentId));
            sends.pause.set(false);
            Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> sends.failed.get() > 0);
            int issuedAtFirstFailure = sends.issued.get();
            Awaitility.await().until(() -> sends.issued.get() > issuedAtFirstFailure + 10 * BURST);
        } finally {
            stopSender(sender, sends);
        }
        allSettled(sends).get(60, TimeUnit.SECONDS);

        // Every send that found the segment gone failed fast on its one producer: none created another.
        assertEquals(producer.segmentProducersCreated.get(), 1);
    }

    @Test
    public void closingTheClientWhileSendsAreInFlightFailsThemWithoutRecreatingProducers() throws Exception {
        PulsarClient client = newClient();
        String topic = newScalableTopic(2);
        ScalableTopicProducer<byte[]> producer = newProducer(client, topic);

        Sends sends = new Sends();
        Thread sender = startSender(producer, sends);
        int created;
        try {
            Awaitility.await().until(() -> sends.segmentsAcked.size() == 2);
            created = producer.segmentProducersCreated.get();
            // Closing the client fails the sends its v4 producers hold; the sender keeps sending
            // meanwhile.
            client.close();
            int issuedAtClose = sends.issued.get();
            Awaitility.await().until(() -> sends.issued.get() > issuedAtClose + 10 * BURST);
        } finally {
            stopSender(sender, sends);
        }

        // Every send is over right away: nothing waits for a layout that is never coming.
        allSettled(sends).get(10, TimeUnit.SECONDS);
        assertTrue(sends.futures.stream().anyMatch(CompletableFuture::isCompletedExceptionally),
                "expected the sends in flight at close to fail");
        assertEquals(producer.segmentProducersCreated.get(), created,
                "no segment producer may be created once the client is closing");
    }
}
