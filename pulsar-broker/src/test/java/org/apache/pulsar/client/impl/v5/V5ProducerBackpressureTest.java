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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * The client memory limit is the only backpressure a V5 producer has. A send that does not fit under
 * it must block the caller (the default) or fail right away, before anything is queued, and whatever
 * the limit was charged must be given back once the send is over.
 */
public class V5ProducerBackpressureTest extends V5ClientBaseTest {

    private static final long MEMORY_LIMIT_BYTES = 1024 * 1024;
    private static final int PAYLOAD_BYTES = 1024;
    private static final int NUM_MESSAGES = 20_000;

    private PulsarClient newClientWithMemoryLimit() throws Exception {
        return track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .memoryLimit(MemorySize.ofBytes(MEMORY_LIMIT_BYTES))
                .build());
    }

    private static MemoryLimitController memoryLimit(PulsarClient client) {
        return ((PulsarClientV5) client).v4Client().getMemoryLimitController();
    }

    private static void assertMemoryReleased(PulsarClient client) {
        // The per-message share is released right after the caller's future completes, on the
        // completion executor, so it may trail the future by an instant.
        Awaitility.await().untilAsserted(() -> assertEquals(memoryLimit(client).currentUsage(), 0));
    }

    private static Throwable failureOf(CompletableFuture<?> future) throws Exception {
        Throwable failure = future.handle((__, ex) -> ex).get(30, TimeUnit.SECONDS);
        return failure instanceof CompletionException ? failure.getCause() : failure;
    }

    @Test(timeOut = 120_000)
    public void asyncSendsBlockTheCallerAtTheMemoryLimit() throws Exception {
        PulsarClient client = newClientWithMemoryLimit();
        String topic = newScalableTopic(1);
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.bytes())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        AsyncProducer<byte[]> async = producer.async();
        byte[] payload = new byte[PAYLOAD_BYTES];

        AtomicInteger completed = new AtomicInteger();
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(NUM_MESSAGES);
        int maxOutstanding = 0;
        for (int i = 0; i < NUM_MESSAGES; i++) {
            CompletableFuture<MessageId> future = async.newMessage().value(payload).send();
            future.whenComplete((__, ___) -> completed.incrementAndGet());
            futures.add(future);
            // The first sends go out before the segment producer even exists. Without admission on
            // the caller's thread they would all queue up here, unbounded.
            maxOutstanding = Math.max(maxOutstanding, i + 1 - completed.get());
        }
        // Each send is charged its payload plus the per-message overhead, and the limiter lets one
        // send go over the limit. One more may be admitted while the previous one still counts as
        // outstanding, since a send's reservation is given back just before its future completes.
        long maxAdmitted = MEMORY_LIMIT_BYTES
                / (PAYLOAD_BYTES + ScalableTopicProducer.PER_MESSAGE_OVERHEAD_BYTES) + 1;
        assertTrue(maxOutstanding <= maxAdmitted + 1,
                "outstanding sends reached " + maxOutstanding + " but the limit admits " + maxAdmitted);

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        assertEquals(completed.get(), NUM_MESSAGES);
        assertMemoryReleased(client);
    }

    /**
     * Send futures complete on the IO thread that received the broker's response, and a send issued
     * from there must never wait for memory: that thread is the one freeing it. With blocking on, a
     * send from an application thread parks at the limit; a send from an IO thread fails fast instead.
     */
    @Test(timeOut = 60_000)
    public void sendFromAnIoThreadFailsFastInsteadOfBlocking() throws Exception {
        PulsarClient client = newClientWithMemoryLimit();
        String topic = newScalableTopic(1);
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.bytes())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        AsyncProducer<byte[]> async = producer.async();
        MemoryLimitController memoryLimit = memoryLimit(client);
        byte[] payload = new byte[PAYLOAD_BYTES];

        AtomicReference<String> continuationThread = new AtomicReference<>();
        CompletableFuture<MessageId> chained = async.newMessage().value(payload).send()
                .thenCompose(__ -> {
                    continuationThread.set(Thread.currentThread().getName());
                    // Take the limit past full for the duration of this send only.
                    memoryLimit.forceReserveMemory(2 * MEMORY_LIMIT_BYTES);
                    try {
                        return async.newMessage().value(payload).send();
                    } finally {
                        memoryLimit.releaseMemory(2 * MEMORY_LIMIT_BYTES);
                    }
                });
        // A parked IO thread would surface here as a timeout.
        Throwable failure = failureOf(chained);
        assertTrue(continuationThread.get().startsWith("pulsar-client-io"),
                "expected the continuation on an IO thread, got " + continuationThread.get());
        assertTrue(failure instanceof PulsarClientException.MemoryBufferIsFullException,
                "unexpected outcome of the send from the IO thread: " + failure);

        // The producer is unharmed once there is room again.
        assertNotNull(producer.newMessage().value(payload).send());
        assertMemoryReleased(client);
    }

    @Test(timeOut = 120_000)
    public void asyncSendsFailFastAtTheMemoryLimitWhenNotBlocking() throws Exception {
        PulsarClient client = newClientWithMemoryLimit();
        String topic = newScalableTopic(1);
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.bytes())
                .topic(topic)
                .blockIfQueueFull(false)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        AsyncProducer<byte[]> async = producer.async();
        byte[] payload = new byte[PAYLOAD_BYTES];

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(NUM_MESSAGES);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            futures.add(async.newMessage().value(payload).send());
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .exceptionally(__ -> null)
                .get(60, TimeUnit.SECONDS);

        int sent = 0;
        int rejected = 0;
        for (CompletableFuture<MessageId> future : futures) {
            Throwable failure = failureOf(future);
            if (failure == null) {
                sent++;
            } else {
                assertTrue(failure instanceof PulsarClientException.MemoryBufferIsFullException,
                        "unexpected send failure: " + failure);
                rejected++;
            }
        }
        assertTrue(sent > 0, "no send went through");
        assertTrue(rejected > 0, "a tight loop must overrun a " + MEMORY_LIMIT_BYTES + " byte limit");
        assertMemoryReleased(client);
    }

    @Test(timeOut = 60_000)
    public void syncSendsAreChargedAndReleased() throws Exception {
        PulsarClient client = newClientWithMemoryLimit();
        String topic = newScalableTopic(1);
        // Batching left on: a synchronous send must flush its batch rather than wait out the delay.
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.bytes())
                .topic(topic)
                .create();
        byte[] payload = new byte[PAYLOAD_BYTES];
        for (int i = 0; i < 100; i++) {
            assertNotNull(producer.newMessage().value(payload).send());
        }
        assertMemoryReleased(client);
    }

    @Test(timeOut = 60_000)
    public void memoryIsReleasedWhenTheSendFails() throws Exception {
        PulsarClient client = newClientWithMemoryLimit();
        String topic = newScalableTopic(1);
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.bytes())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        // Above the broker's maximum message size, so the v4 producer rejects it at enqueue time,
        // on the dispatch thread of this cold segment. It is admitted as the one send the limiter
        // lets go over the limit.
        byte[] tooLarge = new byte[6 * 1024 * 1024];
        byte[] payload = new byte[PAYLOAD_BYTES];
        AtomicReference<String> continuationThread = new AtomicReference<>();
        // The reservation must be given back before the failure reaches the caller, and the caller's
        // continuation must not run on the dispatch thread: a blocking send issued from it would
        // otherwise park that thread behind its own release, queued as the next link.
        CompletableFuture<MessageId> chained = producer.async().newMessage().value(tooLarge).send()
                .exceptionally(ex -> {
                    continuationThread.set(Thread.currentThread().getName());
                    return null;
                })
                .thenCompose(__ -> producer.async().newMessage().value(payload).send());
        assertNotNull(chained.get(30, TimeUnit.SECONDS), "the send issued from the continuation must complete");
        assertFalse(continuationThread.get().startsWith("pulsar-client-internal"),
                "the failure was delivered on the dispatch thread: " + continuationThread.get());
        assertMemoryReleased(client);
    }

    /**
     * The reservation follows the send, not the caller's future: cancelling the future neither hands
     * the budget back while the message is still queued, nor loses it once the send is dropped.
     */
    @Test(timeOut = 60_000)
    public void cancelledSendsGiveTheirReservationBackOnceDropped() throws Exception {
        PulsarClient client = newClientWithMemoryLimit();
        String topic = newScalableTopic(1);
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.bytes())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        byte[] payload = new byte[PAYLOAD_BYTES];

        // Cold segment: these queue up behind the creation of the segment producer.
        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(producer.async().newMessage().value(payload).send());
        }
        futures.forEach(future -> future.cancel(true));
        assertTrue(memoryLimit(client).currentUsage() > 0,
                "cancelling must not hand the budget back while the sends are still queued");

        // Dropped before dispatch or acknowledged after it, each send gives its budget back once.
        assertNotNull(producer.newMessage().value(payload).send());
        assertMemoryReleased(client);
    }

    /**
     * A send whose segment gets sealed by a split is retried on a child. Between the attempts the
     * message stays with this layer, which takes its payload share again; the retries must leave
     * the accounting balanced.
     */
    @Test(timeOut = 120_000)
    public void retriesAcrossASegmentSplitKeepTheAccountingBalanced() throws Exception {
        PulsarClient client = newClientWithMemoryLimit();
        String topic = newScalableTopic(1);
        long parent = singleActiveSegmentId(topic);
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.bytes())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        byte[] payload = new byte[PAYLOAD_BYTES];
        int numMessages = 50_000;

        AtomicInteger completed = new AtomicInteger();
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(numMessages);
        Thread sender = new Thread(() -> {
            for (int i = 0; i < numMessages; i++) {
                CompletableFuture<MessageId> future = producer.async().newMessage().value(payload).send();
                future.whenComplete((__, ___) -> completed.incrementAndGet());
                futures.add(future);
            }
        }, "sender");
        sender.start();
        // Seal the parent while sends are in flight and more are waiting at the memory limit.
        Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).until(() -> completed.get() > 500);
        admin.scalableTopics().splitSegment(topic, parent);
        sender.join();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        assertEquals(completed.get(), numMessages);
        Set<Long> segments = new HashSet<>();
        for (CompletableFuture<MessageId> future : futures) {
            segments.add(((MessageIdV5) future.join()).segmentId());
        }
        assertTrue(segments.size() > 1, "expected sends on the parent and on its children, got " + segments);
        assertMemoryReleased(client);
    }

    private long singleActiveSegmentId(String topic) throws Exception {
        for (var segment : admin.scalableTopics().getMetadata(topic).getSegments().values()) {
            if (segment.isActive()) {
                return segment.getSegmentId();
            }
        }
        throw new AssertionError("no active segment for " + topic);
    }
}
