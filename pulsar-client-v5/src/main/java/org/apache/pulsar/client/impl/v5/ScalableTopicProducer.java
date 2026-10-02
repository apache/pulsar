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

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.EncodeData;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.v5.MessageBuilder;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.EntryBucketBatcherBuilder;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.scalable.ScalableTopicConstants;

/**
 * V5 Producer implementation for scalable topics.
 *
 * <p>Maintains a per-segment v4 ProducerImpl and routes messages by hashing
 * the message key to find the target segment. When the layout changes (split/merge),
 * segment producers are created/closed accordingly.
 *
 * <p>Backpressure: the client memory limit is the only bound on what a V5 producer holds, and it is
 * applied here, on the caller's thread, before a message is queued anywhere. Each send is charged its
 * payload plus {@link #PER_MESSAGE_OVERHEAD_BYTES} up front ({@link #admit}), blocking or failing
 * fast per {@code blockIfQueueFull}; a send from one of the client's IO threads, which is where the
 * send futures complete, never blocks and fails fast at the limit instead. The per-segment v4
 * producers only account for the payload once a message reaches them and never block on the limit
 * themselves ({@link ProducerConfigurationData#isMemoryLimitAdmittedUpstream()}), so nothing on the
 * dispatch chain, whose links may run on an IO thread, ever waits for memory.
 */
final class ScalableTopicProducer<T> implements Producer<T>, DagWatchClient.LayoutChangeListener {

    private static final Logger LOG = Logger.get(ScalableTopicProducer.class);

    /** Max attempts for a send when the target segment is gone (split/merge seal or migration
     *  termination), giving the DAG watch time to deliver the new layout before giving up. */
    private static final int SEND_RETRY_MAX_ATTEMPTS = 10;
    /** Cap on the per-attempt backoff while waiting for the new layout. */
    private static final long SEND_RETRY_MAX_BACKOFF_MS = 500L;
    /** Failure of a send routed on a layout that changed before its segment producer was created. */
    private static final String SEGMENT_NOT_ACTIVE = "not found in active segments";

    // Per-key ordering states of an async send (PendingSend.state).
    /** Not tracked: a synchronous send, or an async send that is done or being re-routed. */
    private static final int SEND_UNTRACKED = 0;
    /** Routed to a segment and not resolved yet. */
    private static final int SEND_IN_FLIGHT = 1;
    /** Its segment is gone: waits to be re-sent, in call order, once the segment has retired. */
    private static final int SEND_PARKED = 2;
    /** A new send held back while a segment retires. */
    private static final int SEND_HELD = 3;

    /** {@link #dispatchSend} argument for a send from the caller, rather than released. */
    private static final long NOT_RELEASED = -1;

    /**
     * Memory charged for each pending message on top of its payload, covering the per-message
     * bookkeeping the client retains until the send completes: message metadata, the v4 send op and
     * its callbacks, the dispatch-chain link and the futures carrying the result. An estimate, whose
     * job is to make the client memory limit bound the number of pending messages as well as their
     * bytes, so that a flood of small messages cannot exhaust the heap while staying under the limit.
     */
    static final int PER_MESSAGE_OVERHEAD_BYTES = 1024;

    private final Logger log;

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final org.apache.pulsar.client.api.Schema<T> v4Schema;
    private final ProducerConfigurationData producerConf;
    private final DagWatchClient dagWatch;
    private final SegmentRouter router;
    private final String topicName;
    private final MemoryLimitController memoryLimit;

    /**
     * Runs the dispatch-chain heads, so that the burst of links queued while a segment producer was
     * being created does not run on the IO thread that completes the creation.
     */
    private final ExecutorService dispatchExecutor;

    /**
     * The client's IO threads. They deliver the acknowledgements that free memory, so a send issued
     * on one of them must never wait for memory (see {@link #admit}).
     */
    private final EventLoopGroup eventLoopGroup;

    /**
     * Where the caller's future is completed when its result did not arrive on an IO thread: on
     * the dispatch thread, the timer, the thread closing the producer. Code chained on the future
     * may block, and a send from it may wait for memory, which none of those threads can afford to
     * do (see {@link #finish}). Results that arrive on an IO thread complete in place.
     */
    private final ExecutorService completionExecutor;

    /**
     * Test hook, run in the dispatch link right after the v4 enqueue and before the result callback
     * is registered: lets a test have the acknowledgement arrive in between.
     */
    @VisibleForTesting
    volatile Consumer<CompletableFuture<?>> afterV4EnqueueHook;

    /** Test hook: how many per-segment v4 producers have been created so far. */
    @VisibleForTesting
    final AtomicInteger segmentProducersCreated = new AtomicInteger();

    /**
     * Per-segment v4 producers. Stored as futures so concurrent send-on-cold-segment
     * calls share a single creation attempt without blocking, and so callers running
     * on a netty IO thread can chain on the future asynchronously instead of forcing
     * a blocking {@code .get()} (which would deadlock against the segment producer's
     * own lookup response, processed on the same IO thread).
     */
    private final ConcurrentHashMap<Long, CompletableFuture<org.apache.pulsar.client.api.Producer<T>>>
            segmentProducers = new ConcurrentHashMap<>();

    /**
     * Per-segment dispatch chain. Each async send appends a link whose sole job
     * is to call {@code v4Producer.sendAsync(...)} (fast, synchronous queue insert)
     * once the previous link completes. This serializes the v4-side dispatch in
     * user-call order, side-stepping JDK CompletableFuture's undefined dependent
     * fire-order — which would otherwise let send N enter the v4 queue before
     * send N-1 when both are dependents of the same not-yet-ready producer
     * future. The chain head completes when the producer is ready; subsequent
     * links complete as soon as their {@code sendAsync} call has returned (they
     * do not wait for broker ack — that's the user-visible future).
     */
    private final ConcurrentHashMap<Long, CompletableFuture<org.apache.pulsar.client.api.Producer<T>>>
            dispatchChains = new ConcurrentHashMap<>();
    private final Object dispatchLock = new Object();

    /**
     * Currently in-flight async sends. {@link #flushAsync()} snapshots and
     * awaits these (each user-visible send future completes on broker ack —
     * exactly the flush guarantee).
     *
     * <p>These are the very futures handed back to the caller by
     * {@code AsyncMessageBuilder.send()}, not upstream stages of them. Tracking an
     * upstream stage instead would break the flush contract: {@code allOf} and the
     * caller-facing stage would both be dependents of that upstream future, and the JDK
     * fires dependents of a completing future in unspecified order — so flush() could
     * complete while a send future the caller holds is still not {@code isDone()}.
     */
    private final Set<CompletableFuture<MessageId>> inFlightSends =
            ConcurrentHashMap.newKeySet();

    /**
     * Guards the per-key ordering state of async sends: the segment each one is routed to, the
     * segments being retired, and the sends held back while they are (see {@link #dispatchSend}).
     * Held only for this bookkeeping and never while calling into a segment producer, so that a v4
     * callback taking it cannot deadlock against a dispatch holding a v4 lock.
     */
    private final Object orderLock = new Object();

    /** The async sends routed to each segment and not yet resolved. Guarded by {@link #orderLock}. */
    private final Map<Long, SegmentSends<T>> segmentSends = new HashMap<>();

    /** New async sends held, in call order, while a segment retires. Guarded by {@link #orderLock}. */
    private final ArrayDeque<PendingSend<T>> heldSends = new ArrayDeque<>();

    /**
     * Whether new async sends are held: set when a segment starts retiring, cleared once the held
     * sends have all been dispatched. Guarded by {@link #orderLock}.
     */
    private boolean holdingSends;

    /**
     * How many layouts were applied. {@link #releaseSends} stops dispatching a batch when it changes:
     * the sends left in the batch would be routed by the new layout, possibly to the successors of a
     * segment that earlier sends of the batch went to, and overtake them there. Guarded by
     * {@link #orderLock}.
     */
    private long layoutChanges;

    /** Call order of the async sends, by which the sends of a retired segment are re-sent. */
    private final AtomicLong sendOrder = new AtomicLong();

    /** Serializes {@link #releaseSends}, so that released sends are dispatched in order. */
    private final Object releaseLock = new Object();
    private final AtomicBoolean releaseScheduled = new AtomicBoolean();

    // Current active segments (volatile for visibility across threads)
    private volatile List<ActiveSegment> activeSegments = List.of();

    private volatile boolean closed = false;
    private final AsyncProducerV5<T> asyncView;

    ScalableTopicProducer(PulsarClientV5 client,
                          Schema<T> v5Schema,
                          ProducerConfigurationData producerConf,
                          DagWatchClient dagWatch,
                          ClientSegmentLayout initialLayout) {
        this.client = client;
        this.v5Schema = v5Schema;
        this.v4Schema = SchemaAdapter.toV4(v5Schema);
        this.producerConf = producerConf;
        this.dagWatch = dagWatch;
        this.router = new SegmentRouter();
        this.topicName = dagWatch.topicName().toString();
        this.log = LOG.with().attr("topic", topicName).build();
        this.memoryLimit = client.v4Client().getMemoryLimitController();
        this.dispatchExecutor = client.v4Client().getInternalExecutorService();
        this.eventLoopGroup = client.v4Client().eventLoopGroup();
        this.completionExecutor = client.v4Client().externalExecutorProvider().getExecutor();
        this.asyncView = new AsyncProducerV5<>(this);

        // Register for layout changes
        dagWatch.setListener(this);

        // Initialize with the current layout
        applyLayout(initialLayout);
    }

    @Override
    public String topic() {
        return topicName;
    }

    @Override
    public String producerName() {
        return producerConf.getProducerName();
    }

    @Override
    public MessageBuilder<T> newMessage() {
        return new MessageBuilderV5<>(this);
    }

    @Override
    public long lastSequenceId() {
        // Reflect the configured initialSequenceId even before any segment producer has
        // been created (segment producers are spun up lazily on first send), so a caller
        // that sets initialSequenceId(N) and immediately reads lastSequenceId() sees N.
        long max = producerConf.getInitialSequenceId() == null
                ? -1L : producerConf.getInitialSequenceId();
        for (var future : segmentProducers.values()) {
            // Best-effort: only consult producers that have finished initializing.
            if (future.isDone() && !future.isCompletedExceptionally()) {
                max = Math.max(max, future.join().getLastSequenceId());
            }
        }
        return max;
    }

    @Override
    public AsyncProducer<T> async() {
        return asyncView;
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Close interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PulsarClientException pce) {
                throw pce;
            }
            throw new PulsarClientException(cause);
        }
    }

    /**
     * One message on its way to a segment producer, together with what the client memory limit
     * was charged for it. The payload share is charged from admission until the message is handed
     * to the v4 producer, which accounts for the payload from then on; the
     * {@link #PER_MESSAGE_OVERHEAD_BYTES} share is charged until the send completes.
     */
    private static final class PendingSend<V> {
        final String key;
        final V value;
        /** The value encoded on the caller's thread; null when the v4 producer must encode it. */
        final EncodeData encoded;
        final int payloadSize;
        final Map<String, String> properties;
        final Instant eventTime;
        final Long sequenceId;
        final Duration deliverAfter;
        final Instant deliverAt;
        final List<String> replicationClusters;
        final Transaction txn;
        /** The caller's future; null for a synchronous send. */
        final CompletableFuture<MessageId> userFuture;
        /** Whether this layer currently holds the payload share of the reservation. */
        final AtomicBoolean payloadHeld = new AtomicBoolean(true);
        /** Whether the send has reached its terminal event and given its reservation back. */
        final AtomicBoolean finished = new AtomicBoolean();
        /**
         * Set while the dispatch link hands the message to the v4 producer. A result that arrives
         * while it is set (a failure raised inside {@code sendAsync}, or an acknowledgement that beat
         * the callback registration) is being delivered on the dispatch thread.
         */
        volatile boolean dispatching;

        // Ordering state of an async send, guarded by orderLock.
        /** Position in call order. */
        long order;
        /** One of the {@code SEND_*} states. */
        int state = SEND_UNTRACKED;
        /** The segment it is in flight to or parked on, in those states. */
        long segmentId = -1;
        /** How many times it was parked because its segment was gone. */
        int attempts;
        /** Why it was last parked: the failure it ends with if it can't be re-sent. */
        Throwable lastFailure;

        PendingSend(String key, V value, EncodeData encoded, int payloadSize,
                    Map<String, String> properties, Instant eventTime, Long sequenceId,
                    Duration deliverAfter, Instant deliverAt, List<String> replicationClusters,
                    Transaction txn, CompletableFuture<MessageId> userFuture) {
            this.key = key;
            this.value = value;
            this.encoded = encoded;
            this.payloadSize = payloadSize;
            this.properties = properties;
            this.eventTime = eventTime;
            this.sequenceId = sequenceId;
            this.deliverAfter = deliverAfter;
            this.deliverAt = deliverAt;
            this.replicationClusters = replicationClusters;
            this.txn = txn;
            this.userFuture = userFuture;
        }
    }

    /**
     * The async sends routed to one segment, guarded by {@link #orderLock}. A segment is retiring
     * once one of its sends failed because the segment is gone, or the layout dropped it while it
     * had sends: from then on no new send is routed anywhere until its in-flight sends are resolved
     * and the new layout is in, and its parked sends have been re-sent.
     */
    private static final class SegmentSends<V> {
        int inFlight;
        final List<PendingSend<V>> parked = new ArrayList<>();
        boolean retiring;

        boolean isIdle() {
            return inFlight == 0 && parked.isEmpty() && !retiring;
        }
    }

    /**
     * Encode the value on the caller's thread, so that the memory limit can be charged the exact
     * payload size before the message is queued, and the schema's CPU work stays off the dispatch
     * thread. Left to the v4 producer for the schemas that cannot encode ahead of it: an
     * {@code AUTO_PRODUCE_BYTES} schema only learns the topic schema once the producer is connected,
     * and a key/value schema splits the value between the key and the payload. Their payload is
     * charged by the byte length when it is known.
     */
    private PendingSend<T> newPendingSend(String key, T value, Map<String, String> properties,
                                          Instant eventTime, Long sequenceId, Duration deliverAfter,
                                          Instant deliverAt, List<String> replicationClusters,
                                          Transaction txn, CompletableFuture<MessageId> userFuture) {
        EncodeData encoded = null;
        int payloadSize = 0;
        if (value != null) {
            if (v4Schema instanceof AutoProduceBytesSchema || v4Schema instanceof KeyValueSchema) {
                payloadSize = value instanceof byte[] bytes ? bytes.length : 0;
            } else {
                encoded = new EncodeData(v4Schema.encode(value));
                payloadSize = encoded.data().length;
            }
        }
        return new PendingSend<>(key, value, encoded, payloadSize, properties, eventTime, sequenceId,
                deliverAfter, deliverAt, replicationClusters, txn, userFuture);
    }

    /**
     * Charge the client memory limit for a send, on the caller's thread and before the message is
     * queued anywhere: this is the producer's backpressure. Blocks until there is room, or fails
     * right away with {@link PulsarClientException.MemoryBufferIsFullException} when
     * {@code blockIfQueueFull} is off.
     *
     * <p>Never parks one of the client's IO threads, whatever the setting: they deliver the
     * acknowledgements that free the memory, so a wait there could last forever. The user-visible
     * futures complete on the IO thread that received the broker's response, which is where a send
     * chained on a send lands; at the limit it fails fast instead. That check only runs once a
     * reservation has failed, so the usual path is a single reservation.
     */
    private void admit(PendingSend<T> send) throws PulsarClientException {
        long bytes = (long) send.payloadSize + PER_MESSAGE_OVERHEAD_BYTES;
        if (memoryLimit.tryReserveMemory(bytes)) {
            return;
        }
        if (!producerConf.isBlockIfQueueFull()) {
            throw new PulsarClientException.MemoryBufferIsFullException("Client memory buffer is full");
        }
        if (isEventLoopThread()) {
            throw new PulsarClientException.MemoryBufferIsFullException(
                    "Client memory buffer is full, and a send from a Pulsar IO thread cannot wait for it");
        }
        try {
            memoryLimit.reserveMemory(bytes);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Interrupted while waiting for client memory", e);
        }
    }

    private boolean isEventLoopThread() {
        for (EventExecutor eventLoop : eventLoopGroup) {
            if (eventLoop.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    /** True once this producer or its client is closing: sends are failed rather than retried. */
    private boolean isShuttingDown() {
        return closed || client.v4Client().isClosed();
    }

    /** The v4 producer accounts for the payload from now on: drop this layer's share of it. */
    private void releasePayloadShare(PendingSend<T> send) {
        if (send.payloadHeld.compareAndSet(true, false)) {
            memoryLimit.releaseMemory(send.payloadSize);
        }
    }

    /**
     * Charge the payload again for a message this layer keeps for another attempt, after the v4
     * producer gave it back. Without blocking: the message was admitted once already.
     */
    private void reholdPayloadShare(PendingSend<T> send) {
        if (send.payloadHeld.compareAndSet(false, true)) {
            memoryLimit.forceReserveMemory(send.payloadSize);
        }
    }

    /** The send is over, one way or the other: release whatever this layer still holds for it. */
    private void releaseAll(PendingSend<T> send) {
        releasePayloadShare(send);
        memoryLimit.releaseMemory(PER_MESSAGE_OVERHEAD_BYTES);
    }

    /**
     * Send a message synchronously with routing. Called by MessageBuilderV5.
     * Returns a MessageIdV5 that includes the segment ID for ack routing.
     */
    MessageIdV5 sendInternal(
            String key, T value, Map<String, String> properties,
            Instant eventTime, Long sequenceId,
            Duration deliverAfter, Instant deliverAt,
            List<String> replicationClusters,
            Transaction txn) throws PulsarClientException {

        PendingSend<T> send = newPendingSend(key, value, properties, eventTime, sequenceId,
                deliverAfter, deliverAt, replicationClusters, txn, null);
        admit(send);
        try {
            PulsarClientException lastError = null;
            for (int attempt = 0; attempt < SEND_RETRY_MAX_ATTEMPTS; attempt++) {
                if (isShuttingDown()) {
                    throw new PulsarClientException.AlreadyClosedException("Producer already closed");
                }
                long segmentId = routeMessage(key);
                try {
                    var producer = getOrCreateSegmentProducer(segmentId);
                    var v4MsgId = sendV4(producer, send);
                    return new MessageIdV5(v4MsgId, segmentId);
                } catch (PulsarClientException e) {
                    // Thrown while (re)creating the per-segment producer — already a V5 exception
                    // (it may wrap a v4 TopicTerminated/AlreadyClosed cause).
                    if (!isSegmentGoneError(e)) {
                        throw e;
                    }
                    lastError = e;
                } catch (org.apache.pulsar.client.api.PulsarClientException e) {
                    // Thrown by the v4 producer's send().
                    if (!isSegmentGoneError(e)) {
                        throw new PulsarClientException(e.getMessage(), e);
                    }
                    lastError = new PulsarClientException(e.getMessage(), e);
                }
                // The target segment is gone: sealed by a split/merge, or terminated by a
                // regular-to-scalable migration. Wait for the DAG watch to deliver the new
                // layout; routeMessage on the next attempt lands on an active child. The dead
                // per-segment producer stays where it is until applyLayout drops it: a send
                // routed to the segment in the meantime fails fast on it, instead of re-creating
                // a producer on a terminated topic for every send that finds it gone.
                log.debug().attr("segmentId", segmentId).attr("attempt", attempt + 1)
                        .log("Target segment gone, waiting for layout update");
                // The message stays with this layer while it waits for the new layout.
                reholdPayloadShare(send);
                try {
                    Thread.sleep(Math.min(100L * (attempt + 1), SEND_RETRY_MAX_BACKOFF_MS));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new PulsarClientException("Interrupted while waiting for layout update", ie);
                }
            }
            throw lastError != null ? lastError
                    : new PulsarClientException("Failed to send after segment termination retries");
        } finally {
            releaseAll(send);
        }
    }

    /**
     * The v4 {@code send()} with the payload accounting handed over in between its steps: enqueue,
     * drop this layer's share of the payload, flush so that a batched message does not wait out the
     * batching delay, then wait for the acknowledgement.
     */
    private org.apache.pulsar.client.api.MessageId sendV4(org.apache.pulsar.client.api.Producer<T> producer,
                                                          PendingSend<T> send)
            throws org.apache.pulsar.client.api.PulsarClientException {
        try {
            var ackFuture = buildV4Message(producer, send).sendAsync();
            if (!ackFuture.isDone()) {
                releasePayloadShare(send);
                producer.flushAsync();
            }
            return ackFuture.get();
        } catch (Exception e) {
            throw org.apache.pulsar.client.api.PulsarClientException.unwrap(e);
        }
    }

    /**
     * True if {@code t} (or one of its causes) signals that the target segment is gone —
     * sealed by a split/merge or terminated by a regular-to-scalable migration — so the send
     * should be retried once the new layout arrives. Handles both the v4 exceptions thrown by
     * {@code send()} and the V5-wrapped exceptions thrown while (re)creating the per-segment
     * producer on a now-terminated topic.
     */
    private static boolean isSegmentGoneError(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException) {
                return true;
            }
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException) {
                return true;
            }
            // The per-segment producer-creation path can surface the broker's terminated /
            // already-closed error as a plain (untyped) PulsarClientException whose message
            // carries the server-side class name; match on that too. A send routed just before
            // a layout change, whose segment is not in the layout by the time its producer is
            // created, is gone the same way and lands on an active segment when re-routed.
            String msg = cause.getMessage();
            if (msg != null
                    && (msg.contains("TopicTerminated") || msg.contains("already terminated")
                        || msg.contains("AlreadyClosed") || msg.contains(SEGMENT_NOT_ACTIVE))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Send a message asynchronously with routing. Called by AsyncMessageBuilderV5.
     * Returns a future of MessageIdV5 that includes the segment ID.
     *
     * <p>The message is admitted against the client memory limit here, on the caller's thread
     * (see {@link #admit}), before it is queued on the dispatch chain. The reservation follows the
     * send's own lifecycle, not the returned future: it is given back by {@link #finish} when the
     * send is acknowledged, fails, or is abandoned, whatever the caller did with the future.
     *
     * <p>The returned future is handed to the caller as-is — no identity {@code thenApply}
     * stage in between — so that {@link #flushAsync()} awaits exactly the futures the caller
     * observes. See {@link #inFlightSends}.
     */
    CompletableFuture<MessageId> sendInternalAsync(
            String key, T value, Map<String, String> properties,
            Instant eventTime, Long sequenceId,
            Duration deliverAfter, Instant deliverAt,
            List<String> replicationClusters,
            Transaction txn) {

        CompletableFuture<MessageId> userFuture = new CompletableFuture<>();
        PendingSend<T> send;
        try {
            send = newPendingSend(key, value, properties, eventTime, sequenceId,
                    deliverAfter, deliverAt, replicationClusters, txn, userFuture);
            admit(send);
        } catch (Exception e) {
            // Nothing was charged or queued: fail right here, on the caller's thread.
            userFuture.completeExceptionally(e);
            return userFuture;
        }
        inFlightSends.add(userFuture);
        // Tracked until the caller-visible future is done, whichever side completes it. Registered
        // before the future is handed out, so it runs after the caller's own continuations.
        userFuture.whenComplete((__, ___) -> inFlightSends.remove(userFuture));
        send.order = sendOrder.getAndIncrement();
        dispatchSend(send, NOT_RELEASED);
        return userFuture;
    }

    /**
     * Route an async send and append it to its segment's dispatch chain, keeping per-key order
     * across layout changes.
     *
     * <p>When a segment is sealed by a split/merge, or terminated by a regular-to-scalable
     * migration, the sends still pending on it fail and must be re-sent to its successors. Newer
     * sends for the same keys must not reach the successors before them. So once a segment is gone
     * it retires: its failed sends are parked rather than retried one by one, and new sends are
     * held, in call order, instead of routed. When the new layout is in and the segment has no send
     * in flight any more, {@link #releaseSends} re-sends the parked sends in call order and then the
     * held ones. Routing and the retirement state change together under {@link #orderLock}, so a
     * send either goes to the old segment and is parked if it fails there, or is held.
     *
     * @param releasedAt {@link #NOT_RELEASED} for a send from the caller, which is held while new
     *                   sends are held; for a send {@link #releaseSends} dispatches, the
     *                   {@link #layoutChanges} count its batch was taken at: such a send is routed
     *                   even while new sends are held, unless a new layout was applied since
     * @return false if a released send was not dispatched because a new layout was applied
     */
    private boolean dispatchSend(PendingSend<T> send, long releasedAt) {
        if (abandonIfDone(send)) {
            return true;
        }
        long segmentId;
        Exception failure = null;
        synchronized (orderLock) {
            // Checked under the lock: closeAsync fails the held sends under it after closing.
            if (isShuttingDown()) {
                failure = new PulsarClientException.AlreadyClosedException("Producer already closed");
                segmentId = -1;
            } else if (releasedAt == NOT_RELEASED && holdingSends) {
                send.state = SEND_HELD;
                heldSends.add(send);
                return true;
            } else if (releasedAt != NOT_RELEASED && releasedAt != layoutChanges) {
                return false;
            } else {
                try {
                    segmentId = routeMessage(send.key);
                    segmentSends.computeIfAbsent(segmentId, __ -> new SegmentSends<>()).inFlight++;
                    send.state = SEND_IN_FLIGHT;
                    send.segmentId = segmentId;
                } catch (Exception e) {
                    failure = e;
                    segmentId = -1;
                }
            }
        }
        if (failure != null) {
            finish(send, null, failure);
            return true;
        }
        final long routedSegmentId = segmentId;

        appendToDispatchChain(routedSegmentId,
                producer -> {
                    if (abandonIfDone(send)) {
                        return;
                    }
                    send.dispatching = true;
                    try {
                        CompletableFuture<org.apache.pulsar.client.api.MessageId> ackFuture;
                        try {
                            ackFuture = buildV4Message(producer, send).sendAsync();
                        } catch (Exception e) {
                            // Only this send fails; the chain stays healthy for the sends behind it.
                            onSendFailure(send, routedSegmentId, e);
                            return;
                        }
                        if (afterV4EnqueueHook != null) {
                            afterV4EnqueueHook.accept(ackFuture);
                        }
                        if (!ackFuture.isDone()) {
                            // Handed over: the v4 producer accounts for the payload from here on. A
                            // send whose result is already in keeps its share until finish().
                            releasePayloadShare(send);
                        }
                        ackFuture.whenComplete((v4MsgId, ex) -> {
                            if (ex == null) {
                                finish(send, new MessageIdV5(v4MsgId, routedSegmentId), null);
                            } else {
                                // Failure from the v4 send (e.g. the segment sealed mid-flight).
                                onSendFailure(send, routedSegmentId, ex);
                            }
                        });
                    } finally {
                        send.dispatching = false;
                    }
                },
                // Failure while (re)creating the per-segment producer — e.g. the partition was
                // terminated by a migration between routing and creation.
                createEx -> onSendFailure(send, routedSegmentId, createEx));
        return true;
    }

    /**
     * An async send failed on {@code segmentId}. If the segment is gone (a split/merge seal or a
     * migration termination) and the send has attempts left, park it and retire the segment, to be
     * re-sent in call order by {@link #releaseSends}; otherwise fail the user-visible future. No new
     * layout is coming once this producer or its client is closing, so their sends fail right away.
     * Covers both the v4 send failure and the per-segment producer-creation failure.
     */
    private void onSendFailure(PendingSend<T> send, long segmentId, Throwable ex) {
        Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
        if (!isSegmentGoneError(cause)) {
            finish(send, null, ex);
            return;
        }
        boolean parked = false;
        boolean startedRetiring = false;
        boolean shuttingDown;
        synchronized (orderLock) {
            if (send.state != SEND_IN_FLIGHT || send.segmentId != segmentId) {
                // Already given up on: finish() untracked it.
                return;
            }
            // Checked under the lock, together with parking: failWaitingSends drains under it once
            // closing is visible, and a send parked after that would wait for a layout that never
            // comes. No new layout is coming once this producer or its client is closing.
            shuttingDown = isShuttingDown();
            if (!shuttingDown) {
                SegmentSends<T> sends = segmentSends.get(segmentId);
                sends.inFlight--;
                if (send.attempts < SEND_RETRY_MAX_ATTEMPTS) {
                    send.attempts++;
                    send.lastFailure = ex;
                    send.state = SEND_PARKED;
                    sends.parked.add(send);
                    // The message stays with this layer until it is re-sent.
                    reholdPayloadShare(send);
                    parked = true;
                } else {
                    send.state = SEND_UNTRACKED;
                    send.segmentId = -1;
                }
                if (!sends.retiring) {
                    sends.retiring = true;
                    holdingSends = true;
                    startedRetiring = true;
                }
            }
        }
        if (shuttingDown) {
            finish(send, null, ex);
            // Closing the client does not close this producer: fail what waits for a layout here.
            failWaitingSends();
            return;
        }
        if (parked) {
            log.debug().attr("segmentId", segmentId).attr("attempt", send.attempts)
                    .log("Target segment gone, re-sending async send in order after layout update");
        } else {
            finish(send, null, ex);
        }
        if (startedRetiring) {
            // The segment failed before the layout dropped it: wait for the new layout.
            scheduleRetirementCheck(segmentId, 1);
        }
        scheduleRelease();
    }

    private void scheduleRelease() {
        if (!releaseScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            dispatchExecutor.execute(this::releaseSends);
        } catch (RejectedExecutionException e) {
            // The client is closing and its executors are gone: nothing will release the waiting sends.
            releaseScheduled.set(false);
            failWaitingSends();
        }
    }

    /**
     * No new layout is coming once this producer or its client is closing: fail the sends waiting
     * for one, parked or held. Closing the client does not close this producer, so besides
     * {@link #closeAsync}, every path that finds the client closing calls this. Idempotent. The sends
     * still in flight fail as their segment producers close.
     */
    private void failWaitingSends() {
        List<PendingSend<T>> waiting = new ArrayList<>();
        synchronized (orderLock) {
            waiting.addAll(heldSends);
            heldSends.clear();
            for (var sends : segmentSends.values()) {
                waiting.addAll(sends.parked);
                sends.parked.clear();
            }
            for (var send : waiting) {
                send.state = SEND_UNTRACKED;
                send.segmentId = -1;
            }
            holdingSends = false;
        }
        for (var send : waiting) {
            finish(send, null, new PulsarClientException.AlreadyClosedException("Producer already closed"));
        }
    }

    /**
     * Re-send the parked sends of the segments that have retired, in call order, and once no
     * segment is retiring any more, the held sends. A segment has retired when the layout no longer
     * has it and none of its sends is in flight. Runs on the dispatch executor, one at a time, and
     * dispatches outside {@link #orderLock}: a send failing right away re-enters it.
     *
     * <p>A batch stops when a new layout is applied while it is dispatched: the rest of the batch
     * would be routed by it, possibly to the successors of a segment its first sends went to. The
     * rest goes back to the head of the held sends: it is older than all of them, and newer than what
     * was dispatched. A send of the batch failing on a segment that is gone while the layout stays
     * does not stop it: the rest goes to the same segments, where any that fail are parked too and
     * re-sent in call order.
     */
    private void releaseSends() {
        synchronized (releaseLock) {
            releaseScheduled.set(false);
            if (isShuttingDown()) {
                failWaitingSends();
                return;
            }
            while (true) {
                List<PendingSend<T>> batch = new ArrayList<>();
                long batchLayout;
                synchronized (orderLock) {
                    if (!holdingSends) {
                        return;
                    }
                    batchLayout = layoutChanges;
                    boolean retiring = false;
                    var it = segmentSends.entrySet().iterator();
                    while (it.hasNext()) {
                        var entry = it.next();
                        SegmentSends<T> sends = entry.getValue();
                        if (!sends.retiring) {
                            continue;
                        }
                        if (sends.inFlight == 0 && !isActive(entry.getKey())) {
                            batch.addAll(sends.parked);
                            it.remove();
                        } else {
                            retiring = true;
                        }
                    }
                    if (!batch.isEmpty()) {
                        // Retired segments of a merge interleave: restore the call order.
                        batch.sort(Comparator.comparingLong(send -> send.order));
                    } else if (retiring) {
                        return;
                    } else if (heldSends.isEmpty()) {
                        holdingSends = false;
                        return;
                    } else {
                        batch.addAll(heldSends);
                        heldSends.clear();
                    }
                    for (var send : batch) {
                        send.state = SEND_UNTRACKED;
                        send.segmentId = -1;
                    }
                }
                for (int i = 0; i < batch.size(); i++) {
                    if (!dispatchSend(batch.get(i), batchLayout)) {
                        boolean closing;
                        synchronized (orderLock) {
                            // failWaitingSends fails the held sends under the lock after closing.
                            closing = isShuttingDown();
                            if (!closing) {
                                for (int j = batch.size() - 1; j >= i; j--) {
                                    var send = batch.get(j);
                                    send.state = SEND_HELD;
                                    heldSends.addFirst(send);
                                }
                            }
                        }
                        if (closing) {
                            for (int j = i; j < batch.size(); j++) {
                                finish(batch.get(j), null,
                                        new PulsarClientException.AlreadyClosedException("Producer already closed"));
                            }
                            failWaitingSends();
                            return;
                        }
                        break;
                    }
                }
            }
        }
    }

    /**
     * A segment whose sends failed because it is gone, while the layout still has it, retires once
     * the new layout drops it. If no new layout comes within the retry budget, its parked sends fail.
     *
     * <p>Runs on the JDK's delayed executor rather than the client's: closing the client shuts its
     * executors down without closing this producer, and the check must still run then to fail the
     * waiting sends. It only does bookkeeping and completes futures, which {@link #finish} hands off.
     */
    private void scheduleRetirementCheck(long segmentId, int check) {
        CompletableFuture.delayedExecutor(Math.min(100L * check, SEND_RETRY_MAX_BACKOFF_MS),
                        TimeUnit.MILLISECONDS)
                .execute(() -> checkRetirement(segmentId, check));
    }

    private void checkRetirement(long segmentId, int check) {
        if (isShuttingDown()) {
            failWaitingSends();
            return;
        }
        List<PendingSend<T>> expired = List.of();
        boolean recheck = false;
        synchronized (orderLock) {
            SegmentSends<T> sends = segmentSends.get(segmentId);
            if (sends == null || !sends.retiring || !isActive(segmentId)) {
                // Retired: releaseSends re-sends its parked sends.
            } else if (check < SEND_RETRY_MAX_ATTEMPTS || sends.inFlight > 0) {
                recheck = true;
            } else {
                expired = new ArrayList<>(sends.parked);
                for (var send : expired) {
                    send.state = SEND_UNTRACKED;
                    send.segmentId = -1;
                }
                segmentSends.remove(segmentId);
            }
        }
        if (recheck) {
            scheduleRetirementCheck(segmentId, check + 1);
            return;
        }
        for (var send : expired) {
            finish(send, null, send.lastFailure);
        }
        scheduleRelease();
    }

    /** Test hook: how many async sends wait to be re-sent or dispatched, parked or held. */
    @VisibleForTesting
    int waitingSendCount() {
        synchronized (orderLock) {
            int waiting = heldSends.size();
            for (var sends : segmentSends.values()) {
                waiting += sends.parked.size();
            }
            return waiting;
        }
    }

    /** Test hook: how many async sends are in flight to a segment. */
    @VisibleForTesting
    int inFlightSendCount() {
        synchronized (orderLock) {
            int inFlight = 0;
            for (var sends : segmentSends.values()) {
                inFlight += sends.inFlight;
            }
            return inFlight;
        }
    }

    private boolean isActive(long segmentId) {
        for (var segment : activeSegments) {
            if (segment.segmentId() == segmentId) {
                return true;
            }
        }
        return false;
    }

    /** Drop a finished async send from the ordering state. */
    private void untrack(PendingSend<T> send) {
        boolean release = false;
        synchronized (orderLock) {
            switch (send.state) {
                case SEND_IN_FLIGHT -> {
                    SegmentSends<T> sends = segmentSends.get(send.segmentId);
                    sends.inFlight--;
                    release = sends.retiring;
                    if (sends.isIdle()) {
                        segmentSends.remove(send.segmentId);
                    }
                }
                case SEND_PARKED -> {
                    SegmentSends<T> sends = segmentSends.get(send.segmentId);
                    sends.parked.remove(send);
                    release = true;
                }
                case SEND_HELD -> heldSends.remove(send);
                default -> {
                }
            }
            send.state = SEND_UNTRACKED;
            send.segmentId = -1;
        }
        if (release) {
            scheduleRelease();
        }
    }

    /**
     * This layer's terminal event for an async send, whatever the caller did with its future in
     * the meantime: give the reservation back first, then complete the caller's future last, so
     * that the budget is already available to whatever the caller chained on it.
     *
     * <p>The caller's continuations run where the future is completed. That is fine on the IO
     * thread that received the result, where a send from them fails fast rather than waits (see
     * {@link #admit}), and nowhere else: not on the dispatch thread, which has the next links
     * queued behind it, nor on the timer or the thread closing the producer. A result delivered
     * inside the dispatch link, whether a failure raised in {@code sendAsync} or an acknowledgement
     * that beat the callback registration, is on the dispatch thread. Those complete on
     * {@link #completionExecutor}.
     */
    private void finish(PendingSend<T> send, MessageId messageId, Throwable failure) {
        if (!send.finished.compareAndSet(false, true)) {
            return;
        }
        untrack(send);
        releaseAll(send);
        if (send.userFuture.isDone()) {
            // Completed by the caller (cancelled, timed out): nothing left to deliver.
            return;
        }
        boolean offload = send.dispatching || (failure != null && !isEventLoopThread());
        if (!offload || client.v4Client().isClosed()) {
            // In place: the result arrived on an IO thread, or the client is closing. Its executors
            // are shut down with it, with shutdownNow(), which drops whatever is still queued: a
            // completion handed to the executor now could be lost and the caller's future never
            // complete. Once the executors are gone, the same happens below.
            complete(send, messageId, failure);
            return;
        }
        try {
            completionExecutor.execute(() -> complete(send, messageId, failure));
        } catch (RejectedExecutionException e) {
            // The client is shutting down and its executors are gone: complete in place.
            complete(send, messageId, failure);
        }
    }

    private static <V> void complete(PendingSend<V> send, MessageId messageId, Throwable failure) {
        if (failure == null) {
            send.userFuture.complete(messageId);
        } else {
            send.userFuture.completeExceptionally(failure);
        }
    }

    /**
     * A caller that completed the future on its own (cancelled it, or timed it out) is no longer
     * waiting for the send: drop it, and give the reservation back.
     */
    private boolean abandonIfDone(PendingSend<T> send) {
        if (!send.userFuture.isDone()) {
            return false;
        }
        finish(send, null, null);
        return true;
    }

    /**
     * Append a dispatch step to the per-segment chain. The chain head is the
     * segment-producer-creation future; subsequent links complete as soon as
     * their {@code dispatchOp} returns (which calls v4 {@code sendAsync} — a
     * fast queue insert), so dispatch order strictly mirrors call order.
     * If the chain itself fails (e.g., segment producer creation failed),
     * {@code onCreateFailure} is invoked so the caller can retry (when the segment
     * is merely gone) or fail the user-visible future.
     */
    private void appendToDispatchChain(long segmentId,
                                       Consumer<org.apache.pulsar.client.api.Producer<T>> dispatchOp,
                                       Consumer<Throwable> onCreateFailure) {
        synchronized (dispatchLock) {
            // The head completes on the IO thread that finishes creating the segment producer, and
            // every link queued behind it would run there in one burst: hop off it first.
            var prev = dispatchChains.computeIfAbsent(segmentId,
                    id -> getOrCreateSegmentProducerAsync(id)
                            .thenApplyAsync(Function.identity(), dispatchExecutor));
            var next = prev.thenApply(producer -> {
                dispatchOp.accept(producer);
                return producer;
            });
            // If the chain link itself faults (creation failure), hand it to the caller.
            next.exceptionally(ex -> {
                onCreateFailure.accept(ex);
                return null;
            });
            dispatchChains.put(segmentId, next);
        }
    }

    private org.apache.pulsar.client.api.TypedMessageBuilder<T> buildV4Message(
            org.apache.pulsar.client.api.Producer<T> producer, PendingSend<T> send) {

        org.apache.pulsar.client.api.transaction.Transaction v4Txn = TransactionV5.unwrap(send.txn);
        var msgBuilder = v4Txn != null ? producer.newMessage(v4Txn) : producer.newMessage();
        if (send.encoded != null) {
            ((TypedMessageBuilderImpl<T>) msgBuilder).encodedValue(send.encoded);
        } else {
            msgBuilder.value(send.value);
        }

        if (send.key != null) {
            msgBuilder.key(send.key);
        }
        if (send.properties != null && !send.properties.isEmpty()) {
            msgBuilder.properties(send.properties);
        }
        if (send.eventTime != null) {
            msgBuilder.eventTime(send.eventTime.toEpochMilli());
        }
        if (send.sequenceId != null) {
            msgBuilder.sequenceId(send.sequenceId);
        }
        if (send.deliverAfter != null) {
            msgBuilder.deliverAfter(send.deliverAfter.toMillis(), TimeUnit.MILLISECONDS);
        }
        if (send.deliverAt != null) {
            msgBuilder.deliverAt(send.deliverAt.toEpochMilli());
        }
        if (send.replicationClusters != null) {
            msgBuilder.replicationClusters(send.replicationClusters);
        }

        return msgBuilder;
    }

    /**
     * Flush all in-flight async sends. Each user-visible send future completes
     * on broker ack, so awaiting them is exactly the "all sends so far have
     * landed" guarantee flush() owes the caller. Snapshotting the set means
     * sends issued *after* this call aren't waited on (matches v4 contract).
     */
    CompletableFuture<Void> flushAsync() {
        var pending = inFlightSends.toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(pending);
    }

    CompletableFuture<Void> closeAsync() {
        closed = true;
        dagWatch.close();

        // No new layout is coming: fail the sends waiting for one. The sends in flight fail as their
        // segment producers close below.
        failWaitingSends();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var future : segmentProducers.values()) {
            // If creation failed, there's nothing to close — swallow so a single bad
            // segment doesn't fail the overall close.
            futures.add(future.thenCompose(p -> p.closeAsync())
                    .exceptionally(__ -> null));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .whenComplete((__, ___) -> {
                    segmentProducers.clear();
                    dispatchChains.clear();
                });
    }

    // --- Layout change handling ---

    @Override
    public void onLayoutChange(ClientSegmentLayout newLayout, ClientSegmentLayout oldLayout) {
        applyLayout(newLayout);
        // After a layout update under an exclusive access mode, we want to claim any
        // newly-introduced segments eagerly so the exclusivity guarantee covers the
        // whole topic, not just segments hit by the next send. Best-effort: this runs
        // off the DagWatchClient callback and any failure is logged; the next send to
        // that segment will surface the error via the normal PulsarClientException
        // path. (The initial-create path uses {@link #eagerAttachInitialAsync} for
        // strict claim.)
        if (requiresExclusiveAttach() && oldLayout != null) {
            CompletableFuture.runAsync(() -> {
                for (var seg : newLayout.activeSegments()) {
                    if (segmentProducers.containsKey(seg.segmentId())) {
                        continue;
                    }
                    try {
                        getOrCreateSegmentProducer(seg.segmentId());
                    } catch (PulsarClientException e) {
                        log.warn().attr("segmentId", seg.segmentId())
                                .exceptionMessage(e)
                                .log("Eager exclusive attach failed; will retry on next send");
                    }
                }
            }, client.v4Client().getInternalExecutorService());
        }
    }

    private void applyLayout(ClientSegmentLayout layout) {
        // Determine which segments are new and which are gone
        Set<Long> newSegmentIds = ConcurrentHashMap.newKeySet();
        for (var seg : layout.activeSegments()) {
            newSegmentIds.add(seg.segmentId());
        }

        // A segment the layout drops while sends are pending on it retires: new sends are held
        // until those are resolved and the failed ones re-sent (see dispatchSend). Done together
        // with the switch to the new layout, so no send is routed by it before the hold is on.
        boolean holding;
        synchronized (orderLock) {
            for (var entry : segmentSends.entrySet()) {
                SegmentSends<T> sends = entry.getValue();
                if (!newSegmentIds.contains(entry.getKey()) && !sends.retiring
                        && (sends.inFlight > 0 || !sends.parked.isEmpty())) {
                    sends.retiring = true;
                    holdingSends = true;
                }
            }
            this.activeSegments = layout.activeSegments();
            layoutChanges++;
            holding = holdingSends;
        }
        if (holding) {
            scheduleRelease();
        }

        // Close producers for segments that are no longer active
        for (var entry : segmentProducers.entrySet()) {
            if (!newSegmentIds.contains(entry.getKey())) {
                log.info().attr("segmentId", entry.getKey())
                        .log("Closing producer for sealed segment");
                var producerFuture = entry.getValue();
                producerFuture
                        .thenCompose(p -> p.closeAsync())
                        .whenComplete((__, ex) -> {
                            // A producer whose creation failed, the segment having been sealed
                            // before it attached, has nothing to close.
                            if (ex != null && !producerFuture.isCompletedExceptionally()) {
                                log.warn().attr("segmentId", entry.getKey())
                                        .exceptionMessage(ex).log("Error closing producer for segment");
                            }
                        });
                segmentProducers.remove(entry.getKey());
                dispatchChains.remove(entry.getKey());
            }
        }

        log.info().attr("epoch", layout.epoch())
                .attr("activeSegments", newSegmentIds).log("Layout applied");
    }

    /**
     * Strict variant of the eager attach used at initial create time: surfaces any
     * exclusivity failure as a {@link PulsarClientException} so {@code create()} fails
     * up front instead of silently deferring the collision to first send.
     */
    CompletableFuture<Void> eagerAttachInitialAsync() {
        if (!requiresExclusiveAttach()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(() -> {
            for (var seg : activeSegments) {
                try {
                    getOrCreateSegmentProducer(seg.segmentId());
                } catch (PulsarClientException e) {
                    throw new CompletionException(e);
                }
            }
        }, client.v4Client().getInternalExecutorService());
    }

    private boolean requiresExclusiveAttach() {
        ProducerAccessMode mode = producerConf.getAccessMode();
        return mode == ProducerAccessMode.Exclusive
                || mode == ProducerAccessMode.ExclusiveWithFencing
                || mode == ProducerAccessMode.WaitForExclusive;
    }

    // --- Internal ---

    private long routeMessage(String key) {
        List<ActiveSegment> segments = activeSegments;
        if (key != null) {
            return router.route(key, segments);
        } else {
            return router.routeRoundRobin(segments);
        }
    }

    /**
     * Async accessor for the per-segment v4 producer. Returns a shared future so
     * concurrent send-on-cold-segment callers race-free funnel through a single
     * creation attempt — and so callers running on a netty IO thread (e.g. the
     * V5 DLQ dispatch off a v4 receive callback) can chain via {@code thenCompose}
     * instead of blocking on {@code .get()} (which would deadlock against the
     * segment producer's own lookup response, processed on the same IO thread).
     */
    private CompletableFuture<org.apache.pulsar.client.api.Producer<T>> getOrCreateSegmentProducerAsync(
            long segmentId) {
        return segmentProducers.computeIfAbsent(segmentId, id -> {
            // Find the segment and the URI to attach the per-segment v4 producer to.
            // Regular segments use the computed segment:// URI; legacy segments (synthetic
            // layouts wrapping an externally managed persistent:// topic) use that URI directly.
            ActiveSegment segment = null;
            for (var seg : activeSegments) {
                if (seg.segmentId() == id) {
                    segment = seg;
                    break;
                }
            }
            if (segment == null) {
                return CompletableFuture.failedFuture(
                        new PulsarClientException("Segment " + id + " " + SEGMENT_NOT_ACTIVE));
            }

            PulsarClientImpl v4Client = client.v4Client();
            // Clone the user-facing producer config so per-segment producers inherit
            // every builder knob (compression, batching, chunking, encryption,
            // initialSequenceId, accessMode, properties, ...) and not just the few
            // fields explicitly carried over.
            var segConf = producerConf.clone();
            segConf.setTopicName(segment.attachTopicName());
            // Sends are admitted against the client memory limit before they reach the segment
            // producer (see admit): it must only account for their bytes, never block on them.
            segConf.setMemoryLimitAdmittedUpstream(true);
            // Only legacy segments wrap a persistent:// topic that the regular-to-scalable
            // migration pre-check (PIP-475) inspects, so mark just those connections as
            // V5-managed — connections to real segment:// topics are never examined.
            if (segment.isLegacy()) {
                segConf.getProperties().put(
                        ScalableTopicConstants.V5_MANAGED_METADATA_KEY,
                        ScalableTopicConstants.V5_MANAGED_METADATA_VALUE);
            }
            if (producerConf.getProducerName() != null
                    && !producerConf.getProducerName().isEmpty()) {
                segConf.setProducerName(producerConf.getProducerName() + "-seg-" + id);
            }
            applyEntryBucketing(segConf, segment);
            segmentProducersCreated.incrementAndGet();
            return v4Client.createSegmentProducerAsync(segConf, v4Schema);
        });
    }

    /**
     * PIP-486: configure a per-segment producer's batching for entry-bucketing. End-to-end encryption
     * disables batching (an encrypted batch can't be reshaped if re-routed across a divergent layout);
     * otherwise, when batching is enabled, group the segment's batches by entry-bucket and stamp each
     * entry's effective entry-bucket hash range. A segment's bucketing is immutable for its life.
     *
     * <p>The stamp is written for every segment, including single-bucket ones (N = 1, e.g. the
     * legacy/synthetic layouts wrapping a regular {@code persistent://} topic): the effective hash
     * range is standalone metadata a consumer or a geo-replicator can use to check whether a batch
     * still lands cleanly in one bucket of a possibly-different target layout, independent of how any
     * single broker dispatches it.
     */
    static void applyEntryBucketing(ProducerConfigurationData segConf, ActiveSegment segment) {
        if (segConf.isEncryptionEnabled()) {
            segConf.setBatchingEnabled(false);
        } else if (segConf.isBatchingEnabled()) {
            segConf.setBatcherBuilder(new EntryBucketBatcherBuilder(segment.entryBucketSplits()));
        }
    }

    /**
     * Sync wrapper around {@link #getOrCreateSegmentProducerAsync}. Only safe to
     * call from user threads (never from a netty IO thread) since it blocks until
     * the segment producer is ready.
     */
    private org.apache.pulsar.client.api.Producer<T> getOrCreateSegmentProducer(long segmentId)
            throws PulsarClientException {
        try {
            return getOrCreateSegmentProducerAsync(segmentId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Interrupted while creating segment producer", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof org.apache.pulsar.client.api.PulsarClientException v4Exc) {
                throw new PulsarClientException(v4Exc.getMessage(), v4Exc);
            }
            if (cause instanceof PulsarClientException v5Exc) {
                throw v5Exc;
            }
            throw new PulsarClientException(cause != null ? cause : e);
        }
    }
}
