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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.AbstractReplicator.State.Disconnected;
import static org.apache.pulsar.broker.service.AbstractReplicator.State.Started;
import static org.apache.pulsar.broker.service.AbstractReplicator.State.Starting;
import static org.apache.pulsar.broker.service.AbstractReplicator.State.Terminated;
import static org.apache.pulsar.broker.service.AbstractReplicator.State.Terminating;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;
import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.Getter;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.AbstractReplicator;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.MessageExpirer;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.OpSendMsgStats;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.SendCallback;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.Codec;

public abstract class PersistentReplicator extends AbstractReplicator
        implements Replicator, ReadEntriesCallback, DeleteCallback, MessageExpirer {

    private static final Logger LOG = Logger.get(PersistentReplicator.class);
    protected final Logger log;

    protected final PersistentTopic topic;
    protected final ManagedCursor cursor;
    protected final String localSchemaTopicName;

    protected Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();
    private final Object dispatchRateLimiterLock = new Object();

    private volatile int readBatchSize;
    private final int readMaxSizeBytes;

    private final int producerQueueThreshold;

    protected final Rate msgOut = new Rate();
    protected final Rate msgExpired = new Rate();

    protected int messageTTLInSeconds = 0;

    private final Backoff readFailureBackoff = Backoff.builder()
            .initialDelay(Duration.ofSeconds(1))
            .maxBackoff(Duration.ofMinutes(1))
            .build();

    private final PersistentMessageExpiryMonitor expiryMonitor;
    // for connected subscriptions, message expiry will be checked if the backlog is greater than this threshold
    private static final int MINIMUM_BACKLOG_FOR_EXPIRY_CHECK = 1000;

    @Getter
    protected final ReplicatorStatsImpl stats = new ReplicatorStatsImpl();

    protected volatile int waitForCursorRewindingRefCnf = 0;

    protected enum ReasonOfWaitForCursorRewinding {
        Failed_Publishing,
        Fetching_Schema,
        Disconnecting,
        Terminating;
    }
    protected ReasonOfWaitForCursorRewinding reasonOfWaitForCursorRewinding = null;

    protected final LinkedList<InFlightTask> inFlightTasks = new LinkedList<>();

    // Guarded by inFlightTasks. Ownership spans cursor invocation and complete batch submission,
    // including synchronous callbacks. ACKs and read completions only publish work to the owner.
    private boolean processingReads;
    private boolean readRequested;
    private boolean cancelReadRequested;
    private boolean rewindRequested;
    private boolean readRetryScheduled;
    private static final int MAX_READ_PROCESSING_STEPS_PER_TURN = 64;

    public PersistentReplicator(String localCluster, PersistentTopic localTopic, ManagedCursor cursor,
                                String remoteCluster, String remoteTopic,
                                BrokerService brokerService, PulsarClientImpl replicationClient,
                                PulsarAdmin replicationAdmin)
            throws PulsarServerException {
        super(localCluster, localTopic, remoteCluster, remoteTopic, localTopic.getReplicatorPrefix(),
                brokerService, replicationClient, replicationAdmin);
        this.log = LOG.with().ctx(super.log).build();
        this.topic = localTopic;
        this.localSchemaTopicName = TopicName.getPartitionedTopicName(localTopicName).toString();
        this.cursor = Objects.requireNonNull(cursor);
        this.expiryMonitor = new PersistentMessageExpiryMonitor(localTopic,
                Codec.decode(cursor.getName()), cursor, null);

        readBatchSize = getMaxReadBatchSize();
        readMaxSizeBytes = brokerService.pulsar().getConfiguration().getDispatcherMaxReadSizeBytes();
        producerQueueThreshold = (int) (producerQueueSize * 0.9);

        this.initializeDispatchRateLimiterIfNeeded();

        startProducer();
    }

    private int getMaxReadBatchSize() {
        return Math.min(producerQueueSize, brokerService.pulsar().getConfiguration().getDispatcherMaxReadBatchSize());
    }

    @Override
    protected void setProducerAndTriggerReadEntries(Producer<byte[]> producer) {
        /**
         * 1. Try change state to {@link Started}.
         * 2. Atoms modify multiple properties if change state success, to avoid another thread get a null value
         *    producer when the state is {@link Started}.
         */
        Pair<Boolean, State> changeStateRes;
        changeStateRes = compareSetAndGetState(Starting, Started);
        if (changeStateRes.getLeft()) {
            if (!(producer instanceof ProducerImpl)) {
                log.error("The partitions count between two clusters is not the same, "
                        + "the replicator can not be created successfully");
                doCloseProducerAsync(producer, () -> {});
                throw new ClassCastException(producer.getClass().getName() + " can not be cast to ProducerImpl");
            }
            this.producer = (ProducerImpl) producer;
            // Trigger a new read.
            log.info("Created replicator producer");
            backOff.reset();
            // activate cursor: so, entries can be cached.
            this.cursor.setActive();

            // read entries
            readMoreEntries();
        } else {
            if (changeStateRes.getRight() == Started) {
                // Since only one task can call "producerBuilder.createAsync()", this scenario is not expected.
                // So print a warn log.
                log.warn("Replicator was already started by another thread while creating the producer, "
                        + "closing the newly created producer");
            } else if (changeStateRes.getRight() == Terminating || changeStateRes.getRight() == Terminated) {
                log.info("Replicator was terminated, closing the producer");
            } else {
                log.error("Replicator state is not expected, closing the producer");
            }
            // Close the producer if change the state fail.
            doCloseProducerAsync(producer, () -> {});
        }
    }

    @Override
    protected Position getReplicatorReadPosition() {
        return cursor.getMarkDeletedPosition();
    }

    @Override
    public long getNumberOfEntriesInBacklog() {
        return cursor.getNumberOfEntriesInBacklog(true);
    }

    @Override
    public boolean hasBacklog() {
        return cursor.hasBacklog();
    }

    public long getMessageExpiredCount() {
        return expiryMonitor.getTotalMessageExpired();
    }

    @Override
    protected void disableReplicatorRead() {
        // deactivate cursor after successfully close the producer
        this.cursor.setInactive();
    }

    private record ReadLimits(int messages, long bytes) {
        public boolean isReadable() {
            return messages > 0 && bytes > 0;
        }
    }

    /**
     * Calculate read limits for a read operation. Takes the rate limiter into account if it's enabled.
     * Also limits to current readBatchSize and readMaxSizeBytes.
     */
    private ReadLimits getReadLimits(int permits) {

        // return 0, if Producer queue is full, it will pause read entries.
        if (permits <= 0) {
            log.debug()
                    .attr("permits", permits)
                    .log("Producer queue is full, pausing reads");
            return new ReadLimits(0, 0);
        }

        long readLimitOnMsg;
        long readLimitOnByte;

        // handle rate limit
        if (dispatchRateLimiter.isPresent() && dispatchRateLimiter.get().isDispatchRateLimitingEnabled()) {
            DispatchRateLimiter rateLimiter = dispatchRateLimiter.get();
            // rateLimiter returns -1 if there is no rate limit configured
            readLimitOnMsg = rateLimiter.getAvailableDispatchRateLimitOnMsg();
            readLimitOnByte = rateLimiter.getAvailableDispatchRateLimitOnByte();
            // no permits from rate limit when either limit is 0
            if (readLimitOnByte == 0 || readLimitOnMsg == 0) {
                log.debug()
                        .attr("dispatchRateOnMsg", rateLimiter.getDispatchRateOnMsg())
                        .attr("dispatchRateOnByte", rateLimiter.getDispatchRateOnByte())
                        .attr("readLimitOnMsg", readLimitOnMsg)
                        .attr("readLimitOnByte", readLimitOnByte)
                        .log("Message-read exceeded topic replicator rate limit");
                return new ReadLimits(-1, -1);
            }
            // use given permits if no rate limit configured, otherwise limit to returned rate limiter permits
            readLimitOnMsg = readLimitOnMsg == -1 ? permits : Math.min(permits, readLimitOnMsg);
            // use readMaxSizeBytes if no rate limit configured, otherwise limit to returned rate limiter permits
            readLimitOnByte = readLimitOnByte == -1 ? readMaxSizeBytes : Math.min(readMaxSizeBytes, readLimitOnByte);
        } else {
            readLimitOnMsg = permits;
            readLimitOnByte = readMaxSizeBytes;
        }

        // limit messages to current read batch size
        readLimitOnMsg = Math.min(readLimitOnMsg, readBatchSize);

        return new ReadLimits((int) readLimitOnMsg, readLimitOnByte);
    }

    public void disconnectIfNoTrafficAndBacklog() {
        // Disabled the feature.
        int threshold = brokerService.getPulsar().getConfig().getBrokerReplicationInactiveThresholdSeconds();
        if (threshold <= 0) {
            return;
        }
        // Has backlog.
        long backlog = getNumberOfEntriesInBacklog();
        if (backlog > 0) {
            return;
        }
        // Already disconnected.
        if (state != Started) {
            return;
        }

        // Disconnect if no backlog and no traffic for a long time.
        if (System.currentTimeMillis() - latestPublishTime > threshold * 1000L) {
            log.info().attr("brokerReplicationInactiveThresholdSeconds", threshold)
                    .log("Disconnecting replication producers since no producer is active for a long time.");
            disconnect();
        }
    }

    protected void readMoreEntries() {
        requestReadProcessing(true);
    }

    private void requestReadProcessing(boolean requestRead) {
        synchronized (inFlightTasks) {
            readRequested |= requestRead;
            if (processingReads) {
                return;
            }
            processingReads = true;
        }
        processReads();
    }

    private void processReads() {
        try {
            for (int i = 0; i < MAX_READ_PROCESSING_STEPS_PER_TURN; i++) {
                if (!processRead()) {
                    return;
                }
            }
            // Keep ownership while yielding, so another callback cannot start a second drain.
            brokerService.executor().execute(this::processReads);
        } catch (Throwable t) {
            log.error().exception(t).log("Unexpected failure processing replication reads");
            boolean terminated;
            synchronized (inFlightTasks) {
                terminated = state == Terminating || state == Terminated;
                if (!terminated) {
                    // Publish recovery before releasing ownership. A concurrent ACK can then
                    // resume this work without reading past an incomplete rewind.
                    inFlightTasks.forEach(task -> task.skipReadResultDueToCursorRewind = true);
                    cancelReadRequested = true;
                    rewindRequested = true;
                    processingReads = false;
                }
            }
            if (terminated) {
                discardPendingReadResults();
                return;
            }
            // No immediate drain here: a repeatedly failing rewind must not recurse.
            delayReadRetry();
        }
    }

    /** Finish terminal cleanup without invoking a cursor operation that may have just failed. */
    private void discardPendingReadResults() {
        while (true) {
            List<Entry> entries = null;
            synchronized (inFlightTasks) {
                for (InFlightTask task : inFlightTasks) {
                    if (!task.submissionComplete && task.entries != null) {
                        entries = task.entries;
                        task.entries = Collections.emptyList();
                        task.submissionComplete = true;
                        break;
                    }
                }
                if (entries == null) {
                    processingReads = false;
                    return;
                }
            }
            entries.forEach(entry -> discardEntry(entry, null));
        }
    }

    private void handleReadRetrySchedulingFailure(Exception exception) {
        // Ownership has already been released. Never clear a newer owner's state here.
        log.error().exception(exception).log("Failed to schedule replication read retry");
        if (exception instanceof RejectedExecutionException) {
            // A rejected retry has no wakeup left if there are no producer ACKs in flight.
            // Do not leave the replicator apparently Started but unable to make progress.
            terminate();
        }
    }

    /** Processes one read, result or recovery transition, with no callback invoked under the state lock. */
    private boolean processRead() {
        InFlightTask task;
        ReadLimits limits = null;
        boolean cancel;
        boolean rewind = false;
        long retryDelayMillis = 0;
        synchronized (inFlightTasks) {
            task = null;
            for (InFlightTask candidate : inFlightTasks) {
                if (!candidate.submissionComplete) {
                    task = candidate;
                    break;
                }
            }
            cancel = cancelReadRequested;
            cancelReadRequested = false;
            if (cancel) {
                // Cancellation belongs to the owner: a concurrent request may have arrived between
                // registering a read and actually invoking the cursor.
            } else if (task != null) {
                if (task.entries == null) {
                    processingReads = false;
                    return false;
                }
                if (!task.skipReadResultDueToCursorRewind && task.readException == null
                        && state != Started && state != Terminating && state != Terminated) {
                    retryDelayMillis = Math.max(100, estimatedTimeStampProducerConnected
                            - System.currentTimeMillis() + 100);
                }
            } else if (rewindRequested) {
                rewindRequested = false;
                rewind = state != Terminating && state != Terminated;
            } else if (!readRequested || state == Terminating || state == Terminated
                    || waitForCursorRewindingRefCnf > 0) {
                processingReads = false;
                return false;
            } else if (state != Started) {
                retryDelayMillis = MESSAGE_RATE_BACKOFF_MS;
            } else {
                int permits = getPermitsIfNoPendingRead();
                if (permits > 0) {
                    limits = getReadLimits(isWritable() ? permits : 1);
                }
                if (limits == null || !limits.isReadable()) {
                    retryDelayMillis = MESSAGE_RATE_BACKOFF_MS;
                } else {
                    readRequested = false;
                    task = createOrRecycleInFlightTaskIntoQueue(cursor.getReadPosition(), limits.messages);
                }
            }
            if (retryDelayMillis > 0) {
                processingReads = false;
            }
        }
        if (retryDelayMillis > 0) {
            try {
                scheduleReadRetry(retryDelayMillis);
                if (state == Disconnected) {
                    startProducer();
                }
            } catch (Exception e) {
                // Ownership was already released: a new owner might be running now. Do not let
                // this failure reach the owner cleanup in processReads and clear its ownership.
                handleReadRetrySchedulingFailure(e);
            }
            return false;
        }
        if (cancel) {
            if (task != null && task.entries == null && cursor.cancelPendingReadRequest()) {
                synchronized (inFlightTasks) {
                    task.entries = Collections.emptyList();
                    task.submissionComplete = true;
                }
            }
        } else if (rewind) {
            cursor.rewind();
        } else if (limits != null) {
            try {
                cursor.asyncReadEntriesOrWait(task.readingEntries, limits.bytes, this, task,
                        topic.getMaxReadPosition());
            } catch (Throwable e) {
                // An unusual cursor implementation may complete its callback and then throw.
                // Keep an already published result and its ownership in that case.
                synchronized (inFlightTasks) {
                    if (task.entries == null) {
                        task.readException = ManagedLedgerException.getManagedLedgerException(e);
                        task.entries = Collections.emptyList();
                    }
                }
            }
        } else if (task != null) {
            processReadResult(task);
        }
        return true;
    }

    private void scheduleReadRetry(long delayMillis) {
        synchronized (inFlightTasks) {
            if (readRetryScheduled || state == Terminating || state == Terminated) {
                return;
            }
            readRetryScheduled = true;
        }
        try {
            brokerService.executor().schedule(() -> {
                synchronized (inFlightTasks) {
                    readRetryScheduled = false;
                }
                readMoreEntries();
            }, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            synchronized (inFlightTasks) {
                readRetryScheduled = false;
            }
            throw e;
        }
    }

    @Override
    public void readEntriesComplete(List<Entry> entries, Object ctx) {
        synchronized (inFlightTasks) {
            ((InFlightTask) ctx).entries = entries;
        }
        requestReadProcessing(false);
    }

    private void processReadResult(InFlightTask task) {
        try {
            if (task.readException != null) {
                handleReadFailure(task.readException, task);
                return;
            }
            latestPublishTime = System.currentTimeMillis();
            if (state == Terminated || state == Terminating || task.skipReadResultDueToCursorRewind) {
                task.entries.forEach(entry -> discardEntry(entry, null));
                synchronized (inFlightTasks) {
                    // Even a stale read may have advanced the cursor after recovery was requested.
                    // Keep admission closed until the owner has discarded it and rewound the cursor.
                    rewindRequested |= state != Terminated && state != Terminating;
                    task.entries = Collections.emptyList();
                }
                return;
            }
            readBatchSize = Math.min(readBatchSize * 2, getMaxReadBatchSize());
            synchronized (inFlightTasks) {
                readFailureBackoff.reduceToHalf();
            }
            boolean sent = replicateEntries(task.entries, task);
            synchronized (inFlightTasks) {
                // Recovery decides when to resume. In particular, an immediately failed schema
                // lookup must not turn this callback into another attempt in the same drain.
                if (!task.skipReadResultDueToCursorRewind) {
                    readRequested |= !sent || isWritable();
                }
            }
        } finally {
            synchronized (inFlightTasks) {
                // A final ACK can arrive before sendAsync returns. It must not make this task
                // recyclable while the submission loop still has a reference to it.
                task.submissionComplete = true;
            }
        }
    }

    /** Settle locally owned entries independently so one cleanup failure cannot strand the remaining batch. */
    protected void discardEntry(Entry entry, MessageImpl<?> message) {
        try {
            entry.release();
        } catch (Throwable e) {
            log.error().exception(e).log("Failed to release discarded replication entry");
        }
        if (message != null) {
            try {
                message.recycle();
            } catch (Throwable e) {
                log.error().exception(e).log("Failed to recycle discarded replication message");
            }
        }
    }

    protected abstract boolean replicateEntries(List<Entry> entries, InFlightTask inFlightTask);

    protected CompletableFuture<SchemaInfo> getSchemaInfo(MessageImpl msg) throws ExecutionException {
        if (msg.getSchemaVersion() == null || msg.getSchemaVersion().length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return client.getSchemaProviderLoadingCache().get(localSchemaTopicName)
                .getSchemaByVersion(msg.getSchemaVersion());
    }

    public void updateCursorState() {
        if (isConnected()) {
            cursor.setActive();
        } else {
            cursor.setInactive();
        }
    }

    protected static final class ProducerSendCallback implements SendCallback {
        private PersistentReplicator replicator;
        private Entry entry;
        private MessageImpl msg;
        private InFlightTask inFlightTask;

        @Override
        public void sendComplete(Throwable exception, OpSendMsgStats opSendMsgStats) {
            if (exception != null && !(exception instanceof PulsarClientException.InvalidMessageException)) {
                replicator.log.error()
                        .attr("inFlightTasks", replicator.inFlightTasks)
                        .attr("pendingQueueSize", replicator.producer.getPendingQueueSize())
                        .exception(exception)
                        .log("Error producing on remote broker");
                // cursor should be rewound since it was incremented when readMoreEntries
                replicator.beforeTerminateOrCursorRewinding(ReasonOfWaitForCursorRewinding.Failed_Publishing);
                replicator.doRewindCursor(false);
                // The failed send has completed from the producer queue perspective. The cursor rewind
                // makes the entry readable again, so this in-flight task must release its permit.
                inFlightTask.incCompletedEntries();
            } else {
                replicator.log.debug()
                        .exception(exception)
                        .log("Message persisted on remote broker");
                inFlightTask.incCompletedEntries();
                replicator.cursor.asyncDelete(entry.getPosition(), replicator, entry.getPosition());
            }
            entry.release();

            // Preserve ACK-driven demand even while a read is pending. The owner still admits only
            // one read, but can use this demand if that read fails before the retry timer fires.
            // Otherwise resume when the occupied queue falls below the configured threshold.
            // If the producer is not currently writable (disconnected or TCP window full), we want to defer the reads
            // until we have emptied the whole queue, and at that point we will read a batch of 1 single message if the
            // producer is still not "writable".
            boolean pendingRead;
            int permits;
            synchronized (replicator.inFlightTasks) {
                pendingRead = replicator.hasPendingRead();
                permits = pendingRead ? 0 : replicator.getPermitsIfNoPendingRead();
            }
            if (pendingRead) {
                replicator.readMoreEntries();
            } else if (replicator.producerQueueSize - permits < replicator.producerQueueThreshold) {
                if (replicator.producerQueueSize == permits || replicator.producer.isWritable()) {
                    replicator.readMoreEntries();
                } else {
                    replicator.log.debug()
                            .attr("pending", replicator.producerQueueSize - permits)
                            .attr("isWritable", replicator.producer.isWritable())
                            .log("Not resuming reads");
                }
            }

            recycle();
        }

        private final Handle<ProducerSendCallback> recyclerHandle;

        private ProducerSendCallback(Handle<ProducerSendCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static ProducerSendCallback create(PersistentReplicator replicator, Entry entry, MessageImpl msg,
                                           InFlightTask inFlightTask) {
            ProducerSendCallback sendCallback = RECYCLER.get();
            sendCallback.replicator = replicator;
            sendCallback.entry = entry;
            sendCallback.msg = msg;
            sendCallback.inFlightTask = inFlightTask;
            return sendCallback;
        }

        private void recycle() {
            inFlightTask = null;
            replicator = null;
            entry = null; //already released and recycled on sendComplete
            if (msg != null) {
                msg.recycle();
                msg = null;
            }
            recyclerHandle.recycle(this);
        }

        private static final Recycler<ProducerSendCallback> RECYCLER = new Recycler<ProducerSendCallback>() {
            @Override
            protected ProducerSendCallback newObject(Handle<ProducerSendCallback> handle) {
                return new ProducerSendCallback(handle);
            }
        };

        @Override
        public void addCallback(MessageImpl<?> msg, SendCallback scb) {
            // noop
        }

        @Override
        public SendCallback getNextSendCallback() {
            return null;
        }

        @Override
        public MessageImpl<?> getNextMessage() {
            return null;
        }

        @Override
        public CompletableFuture<MessageId> getFuture() {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        synchronized (inFlightTasks) {
            InFlightTask task = (InFlightTask) ctx;
            task.readException = exception;
            task.entries = Collections.emptyList();
        }
        requestReadProcessing(false);
    }

    private void handleReadFailure(ManagedLedgerException exception, InFlightTask task) {
        if (state != Started) {
            return;
        }
        if (exception instanceof CursorAlreadyClosedException) {
            log.warn().exception(exception).log("Cursor closed while reading replication entries");
            terminate();
            return;
        }
        readBatchSize = brokerService.pulsar().getConfiguration().getDispatcherMinReadBatchSize();
        long waitTimeMillis = delayReadRetry();
        if (!(exception instanceof TooManyRequestsException)) {
            log.error().attr("task", task).attr("waitTimeMillis", waitTimeMillis).exception(exception)
                    .log("Error reading entries, retrying");
        }
    }

    /**
     * Arrange a fallback retry without generating immediate read demand. Producer acknowledgements
     * can still resume reads before the timer, preserving progress under transient read throttling.
     */
    protected long delayReadRetry() {
        long waitTimeMillis;
        synchronized (inFlightTasks) {
            waitTimeMillis = readFailureBackoff.next().toMillis();
        }
        try {
            scheduleReadRetry(waitTimeMillis);
        } catch (Exception e) {
            // A failed timer must not interrupt the caller's unsent-entry cleanup or schema rewind.
            handleReadRetrySchedulingFailure(e);
        }
        return waitTimeMillis;
    }

    public CompletableFuture<Void> clearBacklog() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        log.debug()
                .attr("backlog", cursor.getNumberOfEntriesInBacklog(false))
                .log("Backlog size before clearing");

        cursor.asyncClearBacklog(new ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                log.debug()
                        .attr("backlog", cursor.getNumberOfEntriesInBacklog(false))
                        .log("Backlog size after clearing");
                future.complete(null);
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                log.error().exception(exception).log("Failed to clear backlog");
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        log.debug()
                .attr("numMessagesToSkip", numMessagesToSkip)
                .attr("backlog", cursor.getNumberOfEntriesInBacklog(false))
                .log("Skipping messages");
        cursor.asyncSkipEntries(numMessagesToSkip, IndividualDeletedEntries.Exclude,
                new AsyncCallbacks.SkipEntriesCallback() {
                    @Override
                    public void skipEntriesComplete(Object ctx) {
                        log.debug()
                                .attr("numMessagesToSkip", numMessagesToSkip)
                                .attr("backlog", cursor.getNumberOfEntriesInBacklog(false))
                                .log("Skipped messages");
                        future.complete(null);
                    }

                    @Override
                    public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error()
                                .attr("numMessagesToSkip", numMessagesToSkip)
                                .exception(exception)
                                .log("Failed to skip messages");
                        future.completeExceptionally(exception);
                    }
                }, null);

        return future;
    }

    public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        log.debug()
                .attr("messagePosition", messagePosition)
                .log("Getting message at position");

        cursor.asyncGetNthEntry(messagePosition, IndividualDeletedEntries.Exclude, new ReadEntryCallback() {

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                future.complete(entry);
            }

            @Override
            public String toString() {
                return String.format("Replication [%s] peek Nth message",
                        PersistentReplicator.this.producer.getProducerName());
            }
        }, null);

        return future;
    }

    @Override
    public void deleteComplete(Object ctx) {
        log.debug().attr("ctx", ctx).log("Deleted message");
    }

    @Override
    public void deleteFailed(ManagedLedgerException exception, Object ctx) {
        log.error()
                .attr("ctx", ctx)
                .exception(exception)
                .log("Failed to delete message");
        if (exception instanceof CursorAlreadyClosedException) {
            log.warn()
                    .attr("ctx", ctx)
                    .exception(exception)
                    .log("Asynchronous ack failure because replicator is already deleted "
                            + "and cursor is already closed");
            // replicator is already deleted and cursor is already closed so, producer should also be disconnected.
            terminate();
            return;
        }
        if (ctx instanceof Position) {
            Position deletedEntry = (Position) ctx;
            if (deletedEntry.compareTo(cursor.getMarkDeletedPosition()) > 0) {
                brokerService.getPulsar().getExecutor().schedule(
                        () -> cursor.asyncDelete(deletedEntry, this, deletedEntry), 10,
                        TimeUnit.SECONDS);
            }
        }
    }

    public void updateRates() {
        msgOut.calculateRate();
        msgExpired.calculateRate();
        expiryMonitor.updateRates();

        stats.msgRateOut = msgOut.getRate();
        stats.msgThroughputOut = msgOut.getValueRate();
        stats.msgRateExpired = msgExpired.getRate() + expiryMonitor.getMessageExpiryRate();
    }

    public ReplicatorStatsImpl computeStats() {
        stats.replicationBacklog = cursor.getNumberOfEntriesInBacklog(false);
        stats.connected = isConnected();
        stats.replicationDelayInSeconds = TimeUnit.MILLISECONDS.toSeconds(getReplicationDelayMs());

        ProducerImpl producer = this.producer;
        if (producer != null) {
            stats.outboundConnection = producer.getConnectionId();
            stats.outboundConnectedSince = producer.getConnectedSince();
        } else {
            stats.outboundConnection = null;
            stats.outboundConnectedSince = null;
        }

        return stats;
    }

    public void updateMessageTTL(int messageTTLInSeconds) {
        this.messageTTLInSeconds = messageTTLInSeconds;
    }

    @Override
    public boolean expireMessages(int messageTTLInSeconds) {
        long backlog = cursor.getNumberOfEntriesInBacklog(false);
        if ((backlog == 0) || (backlog < MINIMUM_BACKLOG_FOR_EXPIRY_CHECK
                        && !topic.isOldestMessageExpired(cursor, messageTTLInSeconds))) {
            // don't do anything for almost caught-up connected subscriptions
            return false;
        }

        return expiryMonitor.expireMessages(messageTTLInSeconds);
    }

    @Override
    public CompletableFuture<Boolean> expireMessagesAsync(int messageTTLInSeconds) {
        long backlog = cursor.getNumberOfEntriesInBacklog(false);
        if (backlog == 0) {
            return CompletableFuture.completedFuture(false);
        } else if (backlog < MINIMUM_BACKLOG_FOR_EXPIRY_CHECK) {
            return topic.isOldestMessageExpiredAsync(cursor, messageTTLInSeconds).thenCompose(oldestMsgExpired -> {
                if (oldestMsgExpired) {
                    return expiryMonitor.expireMessagesAsync(messageTTLInSeconds);
                } else {
                    return CompletableFuture.completedFuture(false);
                }
            });
        }
        return expiryMonitor.expireMessagesAsync(messageTTLInSeconds);
    }

    @Override
    public boolean expireMessages(Position position) {
        return expiryMonitor.expireMessages(position);
    }

    @Override
    public Optional<DispatchRateLimiter> getRateLimiter() {
        return dispatchRateLimiter;
    }

    @Override
    public void initializeDispatchRateLimiterIfNeeded() {
        synchronized (dispatchRateLimiterLock) {
            if (!dispatchRateLimiter.isPresent()
                    && DispatchRateLimiter.isDispatchRateEnabled(topic.getReplicatorDispatchRate())) {
                this.dispatchRateLimiter = Optional.of(
                        topic.getBrokerService().getDispatchRateLimiterFactory()
                                .createReplicatorDispatchRateLimiter(topic, Codec.decode(cursor.getName())));
            }
        }
    }

    @Override
    public void updateRateLimiter() {
        initializeDispatchRateLimiterIfNeeded();
        dispatchRateLimiter.ifPresent(DispatchRateLimiter::updateDispatchRate);
    }

    protected void checkReplicatedSubscriptionMarker(Position position, MessageImpl<?> msg, ByteBuf payload) {
        if (!msg.getMessageBuilder().hasMarkerType()) {
            // No marker is defined
            return;
        }

        int markerType = msg.getMessageBuilder().getMarkerType();

        if (!(msg.getMessageBuilder().hasReplicatedFrom()
                && remoteCluster.equals(msg.getMessageBuilder().getReplicatedFrom()))) {
            // Only consider markers that are coming from the same cluster that this
            // replicator instance is assigned to.
            // All the replicators will see all the markers, but we need to only process
            // it once.
            return;
        }

        switch (markerType) {
        case MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST_VALUE:
        case MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE_VALUE:
        case MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE:
            topic.receivedReplicatedSubscriptionMarker(position, markerType, payload);
            break;

        default:
            // Do nothing
        }
    }

    @Override
    protected void doReleaseResources() {
        dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);
    }
    @VisibleForTesting
    public ManagedCursor getCursor() {
        return cursor;
    }

    @Data
    protected static class InFlightTask {
        Position readPos;
        int readingEntries;
        volatile List<Entry> entries;
        volatile int completedEntries;
        volatile boolean submissionComplete;
        ManagedLedgerException readException;
        volatile boolean skipReadResultDueToCursorRewind;
        final String replicatorId;

        public synchronized void incCompletedEntries() {
            if (!CollectionUtils.isEmpty(entries) && completedEntries < entries.size()) {
                completedEntries++;
            } else {
                LOG.error()
                        .attr("replicatorId", replicatorId)
                        .attr("value", this.toString())
                        .log("Unexpected calling of increase completed entries");
            }
        }

        synchronized void recycle(Position readStart, int readingEntries) {
            this.readPos = readStart;
            this.readingEntries = readingEntries;
            this.entries = null;
            this.completedEntries = 0;
            this.submissionComplete = false;
            this.readException = null;
            this.skipReadResultDueToCursorRewind = false;
        }

        public InFlightTask(Position readPos, int readingEntries, String replicatorId) {
            this.readPos = readPos;
            this.readingEntries = readingEntries;
            this.replicatorId = replicatorId;
        }

        public boolean isDone() {
            if (!submissionComplete || entries == null) {
                return false;
            }
            if (entries.isEmpty()) {
                return true;
            }
            return completedEntries >= entries.size();
        }

        @Override
        public String toString() {
            return "Replicator InFlightTask "
                + "{replicatorId=" + replicatorId
                + ", readPos=" + readPos
                + ", readingEntries=" + readingEntries
                + ", readoutEntries=" + (entries == null ? "-1" : entries.size())
                + ", completedEntries=" + completedEntries
                + ", skipReadResultDueToCursorRewound=" + skipReadResultDueToCursorRewind
                + "}";
        }
    }

    @VisibleForTesting
    InFlightTask createOrRecycleInFlightTaskIntoQueue(Position readPos, int readingEntries) {
        synchronized (inFlightTasks) {
            // Reuse projects that has done.
            if (!inFlightTasks.isEmpty()) {
                InFlightTask first = inFlightTasks.peek();
                if (first.isDone()) {
                    // Remove from the first index, and add to the latest index.
                    inFlightTasks.poll();
                    first.recycle(readPos, readingEntries);
                    inFlightTasks.add(first);
                    return first;
                }
            }
            // New project if nothing can be reused.
            InFlightTask task = new InFlightTask(readPos, readingEntries, replicatorId);
            inFlightTasks.add(task);
            return task;
        }
    }

    protected int getPermitsIfNoPendingRead() {
        synchronized (inFlightTasks) {
            for (InFlightTask task : inFlightTasks) {
                boolean hasPendingCursorRead = task.readPos != null && task.entries == null;
                if (hasPendingCursorRead) {
                    // Skip the current reading if there is a pending cursor reading.
                    return 0;
                }
            }
            return producerQueueSize - getInflightMessagesCount();
        }
    }

    protected int getInflightMessagesCount() {
        int inFlight = 0;
        synchronized (inFlightTasks) {
            for (InFlightTask task : inFlightTasks) {
                if (task.isDone()) {
                    continue;
                }
                if (task.entries == null) {
                    inFlight += task.readingEntries;
                    continue;
                }
                inFlight += Math.max(task.entries.size() - task.completedEntries, 0);
            }
        }
        return inFlight;
    }

    protected CompletableFuture<Void> beforeDisconnect() {
        // Ensure no in-flight task.
        synchronized (inFlightTasks) {
            for (PersistentReplicator.InFlightTask task : inFlightTasks) {
                if (!task.isDone() && task.readPos.compareTo(cursor.getManagedLedger().getLastConfirmedEntry()) < 0) {
                    return CompletableFuture.failedFuture(new BrokerServiceException
                            .TopicBusyException("Cannot close a replicator with backlog"));
                }
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    protected void beforeTerminateOrCursorRewinding(ReasonOfWaitForCursorRewinding reason) {
        synchronized (inFlightTasks) {
            reasonOfWaitForCursorRewinding = reason;
            waitForCursorRewindingRefCnf++;
            for (InFlightTask task : inFlightTasks) {
                task.skipReadResultDueToCursorRewind = true;
            }
            cancelReadRequested = true;
        }
        requestReadProcessing(false);
    }

    protected void doRewindCursor(boolean triggerReadMoreEntries) {
        synchronized (inFlightTasks) {
            rewindRequested = true;
            waitForCursorRewindingRefCnf--;
            reasonOfWaitForCursorRewinding = null;
        }
        requestReadProcessing(triggerReadMoreEntries);
    }

    @Override
    public void beforeTerminate() {
        beforeTerminateOrCursorRewinding(ReasonOfWaitForCursorRewinding.Terminating);
    }

    /** Test seam for observing the cursor-read reservation; production admission uses the processing owner. */
    @VisibleForTesting
    protected boolean hasPendingRead() {
        synchronized (inFlightTasks) {
            for (InFlightTask task : inFlightTasks) {
                // The purpose of calling "getReadPos" instead of calling "readPos" is to make the test
                // "testReplicationTaskStoppedAfterTopicClosed" can counter the calling times of "readMoreEntries".
                if (task.getReadPos() != null && task.entries == null) {
                    // Skip the current reading if there is a pending cursor reading.
                    return true;
                }
            }
        }
        return false;
    }

    @VisibleForTesting
    String getReplicatorId() {
        return  replicatorId;
    }
}
