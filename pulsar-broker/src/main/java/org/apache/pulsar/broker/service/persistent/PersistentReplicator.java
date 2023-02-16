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

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.AbstractReplicator;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.Type;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.SendCallback;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PersistentReplicator extends AbstractReplicator
        implements Replicator, ReadEntriesCallback, DeleteCallback {

    protected final PersistentTopic topic;
    protected final ManagedCursor cursor;

    protected Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();
    private final Object dispatchRateLimiterLock = new Object();

    private int readBatchSize;
    private final int readMaxSizeBytes;

    private final int producerQueueThreshold;

    protected static final AtomicIntegerFieldUpdater<PersistentReplicator> PENDING_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater
                    .newUpdater(PersistentReplicator.class, "pendingMessages");
    private volatile int pendingMessages = 0;

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    private static final AtomicIntegerFieldUpdater<PersistentReplicator> HAVE_PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater
                    .newUpdater(PersistentReplicator.class, "havePendingRead");
    private volatile int havePendingRead = FALSE;

    protected final Rate msgOut = new Rate();
    protected final Rate msgExpired = new Rate();

    protected int messageTTLInSeconds = 0;

    private final Backoff readFailureBackoff = new Backoff(1, TimeUnit.SECONDS,
            1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private final PersistentMessageExpiryMonitor expiryMonitor;
    // for connected subscriptions, message expiry will be checked if the backlog is greater than this threshold
    private static final int MINIMUM_BACKLOG_FOR_EXPIRY_CHECK = 1000;

    private final ReplicatorStatsImpl stats = new ReplicatorStatsImpl();

    protected volatile boolean fetchSchemaInProgress = false;

    public PersistentReplicator(String localCluster, PersistentTopic localTopic, ManagedCursor cursor,
                                   String remoteCluster, String remoteTopic,
                                   BrokerService brokerService, PulsarClientImpl replicationClient)
            throws PulsarServerException {
        super(localCluster, localTopic.getName(), remoteCluster, remoteTopic, localTopic.getReplicatorPrefix(),
                brokerService, replicationClient);
        this.topic = localTopic;
        this.cursor = cursor;
        this.expiryMonitor = new PersistentMessageExpiryMonitor(localTopicName,
                Codec.decode(cursor.getName()), cursor, null);
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        PENDING_MESSAGES_UPDATER.set(this, 0);

        readBatchSize = Math.min(
                producerQueueSize,
                localTopic.getBrokerService().pulsar().getConfiguration().getDispatcherMaxReadBatchSize());
        readMaxSizeBytes = localTopic.getBrokerService().pulsar().getConfiguration().getDispatcherMaxReadSizeBytes();
        producerQueueThreshold = (int) (producerQueueSize * 0.9);

        this.initializeDispatchRateLimiterIfNeeded();

        startProducer();
    }

    @Override
    protected void readEntries(Producer<byte[]> producer) {
        // Rewind the cursor to be sure to read again all non-acked messages sent while restarting
        cursor.rewind();

        cursor.cancelPendingReadRequest();
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        this.producer = (ProducerImpl) producer;

        if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Started)) {
            log.info("[{}] Created replicator producer", replicatorId);
            backOff.reset();
            // activate cursor: so, entries can be cached
            this.cursor.setActive();
            // read entries
            readMoreEntries();
        } else {
            log.info(
                    "[{}] Replicator was stopped while creating the producer."
                            + " Closing it. Replicator state: {}",
                    replicatorId, STATE_UPDATER.get(this));
            STATE_UPDATER.set(this, State.Stopping);
            closeProducerAsync();
        }

    }

    @Override
    protected Position getReplicatorReadPosition() {
        return cursor.getMarkDeletedPosition();
    }

    @Override
    protected long getNumberOfEntriesInBacklog() {
        return cursor.getNumberOfEntriesInBacklog(false);
    }

    @Override
    protected void disableReplicatorRead() {
        if (this.cursor != null) {
            // deactivate cursor after successfully close the producer
            this.cursor.setInactive();
        }
    }

    /**
     * Calculate available permits for read entries.
     *
     * @return
     *   0:  Producer queue is full, no permits.
     *  -1:  Rate Limiter reaches limit.
     *  >0:  available permits for read entries.
     */
    private int getAvailablePermits() {
        int availablePermits = producerQueueSize - PENDING_MESSAGES_UPDATER.get(this);

        // return 0, if Producer queue is full, it will pause read entries.
        if (availablePermits <= 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Producer queue is full, availablePermits: {}, pause reading",
                        replicatorId, availablePermits);
            }
            return 0;
        }

        // handle rate limit
        if (dispatchRateLimiter.isPresent() && dispatchRateLimiter.get().isDispatchRateLimitingEnabled()) {
            DispatchRateLimiter rateLimiter = dispatchRateLimiter.get();
            // no permits from rate limit
            if (!rateLimiter.hasMessageDispatchPermit()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] message-read exceeded topic replicator message-rate {}/{},"
                                    + " schedule after a {}",
                            replicatorId,
                            rateLimiter.getDispatchRateOnMsg(),
                            rateLimiter.getDispatchRateOnByte(),
                            MESSAGE_RATE_BACKOFF_MS);
                }
                return -1;
            }

            // if dispatch-rate is in msg then read only msg according to available permit
            long availablePermitsOnMsg = rateLimiter.getAvailableDispatchRateLimitOnMsg();
            if (availablePermitsOnMsg > 0) {
                availablePermits = Math.min(availablePermits, (int) availablePermitsOnMsg);
            }
        }

        return availablePermits;
    }

    protected void readMoreEntries() {
        if (fetchSchemaInProgress) {
            log.info("[{}] Skip the reading due to new detected schema", replicatorId);
            return;
        }
        int availablePermits = getAvailablePermits();

        if (availablePermits > 0) {
            int messagesToRead = Math.min(availablePermits, readBatchSize);
            if (!isWritable()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Throttling replication traffic because producer is not writable", replicatorId);
                }
                // Minimize the read size if the producer is disconnected or the window is already full
                messagesToRead = 1;
            }

            // If messagesToRead is 0 or less, correct it to 1 to prevent IllegalArgumentException
            messagesToRead = Math.max(messagesToRead, 1);

            // Schedule read
            if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule read of {} messages", replicatorId, messagesToRead);
                }
                cursor.asyncReadEntriesOrWait(messagesToRead, readMaxSizeBytes, this,
                        null, topic.getMaxReadPosition());
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Not scheduling read due to pending read. Messages To Read {}",
                            replicatorId, messagesToRead);
                }
            }
        } else if (availablePermits == -1) {
            // no permits from rate limit
            topic.getBrokerService().executor().schedule(
                () -> readMoreEntries(), MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No Permits for reading. availablePermits: {}",
                        replicatorId, availablePermits);
            }
        }
    }

    @Override
    public void readEntriesComplete(List<Entry> entries, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Read entries complete of {} messages", replicatorId, entries.size());
        }

        int maxReadBatchSize = topic.getBrokerService().pulsar().getConfiguration().getDispatcherMaxReadBatchSize();
        if (readBatchSize < maxReadBatchSize) {
            int newReadBatchSize = Math.min(readBatchSize * 2, maxReadBatchSize);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Increasing read batch size from {} to {}", replicatorId, readBatchSize,
                        newReadBatchSize);
            }

            readBatchSize = newReadBatchSize;
        }

        readFailureBackoff.reduceToHalf();

        boolean atLeastOneMessageSentForReplication = replicateEntries(entries);

        HAVE_PENDING_READ_UPDATER.set(this, FALSE);

        if (atLeastOneMessageSentForReplication && !isWritable()) {
            // Don't read any more entries until the current pending entries are persisted
            if (log.isDebugEnabled()) {
                log.debug("[{}] Pausing replication traffic. at-least-one: {} is-writable: {}", replicatorId,
                        atLeastOneMessageSentForReplication, isWritable());
            }
        } else {
            readMoreEntries();
        }
    }

    protected abstract boolean replicateEntries(List<Entry> entries);

    protected CompletableFuture<SchemaInfo> getSchemaInfo(MessageImpl msg) throws ExecutionException {
        if (msg.getSchemaVersion() == null || msg.getSchemaVersion().length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return client.getSchemaProviderLoadingCache().get(localTopicName)
                .getSchemaByVersion(msg.getSchemaVersion());
    }

    public void updateCursorState() {
        if (this.cursor != null) {
            if (producer != null && producer.isConnected()) {
                this.cursor.setActive();
            } else {
                this.cursor.setInactive();
            }
        }
    }

    protected static final class ProducerSendCallback implements SendCallback {
        private PersistentReplicator replicator;
        private Entry entry;
        private MessageImpl msg;

        @Override
        public void sendComplete(Exception exception) {
            if (exception != null && !(exception instanceof PulsarClientException.InvalidMessageException)) {
                log.error("[{}] Error producing on remote broker", replicator.replicatorId, exception);
                // cursor should be rewinded since it was incremented when readMoreEntries
                replicator.cursor.rewind();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Message persisted on remote broker", replicator.replicatorId, exception);
                }
                replicator.cursor.asyncDelete(entry.getPosition(), replicator, entry.getPosition());
            }
            entry.release();

            int pending = PENDING_MESSAGES_UPDATER.decrementAndGet(replicator);

            // In general, we schedule a new batch read operation when the occupied queue size gets smaller than half
            // the max size, unless another read operation is already in progress.
            // If the producer is not currently writable (disconnected or TCP window full), we want to defer the reads
            // until we have emptied the whole queue, and at that point we will read a batch of 1 single message if the
            // producer is still not "writable".
            if (pending < replicator.producerQueueThreshold //
                    && HAVE_PENDING_READ_UPDATER.get(replicator) == FALSE //
            ) {
                if (pending == 0 || replicator.producer.isWritable()) {
                    replicator.readMoreEntries();
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Not resuming reads. pending: {} is-writable: {}",
                                replicator.replicatorId, pending,
                                replicator.producer.isWritable());
                    }
                }
            }

            recycle();
        }

        private final Handle<ProducerSendCallback> recyclerHandle;

        private ProducerSendCallback(Handle<ProducerSendCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static ProducerSendCallback create(PersistentReplicator replicator, Entry entry, MessageImpl msg) {
            ProducerSendCallback sendCallback = RECYCLER.get();
            sendCallback.replicator = replicator;
            sendCallback.entry = entry;
            sendCallback.msg = msg;
            return sendCallback;
        }

        private void recycle() {
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
        if (STATE_UPDATER.get(this) != State.Started) {
            log.info("[{}] Replicator was stopped while reading entries."
                            + " Stop reading. Replicator state: {}",
                    replicatorId, STATE_UPDATER.get(this));
            return;
        }

        // Reduce read batch size to avoid flooding bookies with retries
        readBatchSize = topic.getBrokerService().pulsar().getConfiguration().getDispatcherMinReadBatchSize();

        long waitTimeMillis = readFailureBackoff.next();

        if (exception instanceof CursorAlreadyClosedException) {
            log.error("[{}] Error reading entries because replicator is"
                            + " already deleted and cursor is already closed {}, ({})",
                    replicatorId, ctx, exception.getMessage(), exception);
            // replicator is already deleted and cursor is already closed so, producer should also be stopped
            closeProducerAsync();
            return;
        } else if (!(exception instanceof TooManyRequestsException)) {
            log.error("[{}] Error reading entries at {}. Retrying to read in {}s. ({})",
                    replicatorId, ctx, waitTimeMillis / 1000.0, exception.getMessage(), exception);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Throttled by bookies while reading at {}. Retrying to read in {}s. ({})",
                        replicatorId, ctx, waitTimeMillis / 1000.0, exception.getMessage(),
                        exception);
            }
        }

        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        brokerService.executor().schedule(this::readMoreEntries, waitTimeMillis, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> clearBacklog() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Backlog size before clearing: {}", replicatorId,
                    cursor.getNumberOfEntriesInBacklog(false));
        }

        cursor.asyncClearBacklog(new ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Backlog size after clearing: {}", replicatorId,
                            cursor.getNumberOfEntriesInBacklog(false));
                }
                future.complete(null);
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}] Failed to clear backlog", replicatorId, exception);
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Skipping {} messages, current backlog {}", replicatorId,
                    numMessagesToSkip, cursor.getNumberOfEntriesInBacklog(false));
        }
        cursor.asyncSkipEntries(numMessagesToSkip, IndividualDeletedEntries.Exclude,
                new AsyncCallbacks.SkipEntriesCallback() {
                    @Override
                    public void skipEntriesComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Skipped {} messages, new backlog {}", replicatorId,
                                    numMessagesToSkip, cursor.getNumberOfEntriesInBacklog(false));
                        }
                        future.complete(null);
                    }

                    @Override
                    public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}] Failed to skip {} messages", replicatorId,
                                numMessagesToSkip, exception);
                        future.completeExceptionally(exception);
                    }
                }, null);

        return future;
    }

    public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Getting message at position {}", replicatorId,
                    messagePosition);
        }

        cursor.asyncGetNthEntry(messagePosition, IndividualDeletedEntries.Exclude, new ReadEntryCallback() {

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                future.complete(entry);
            }
        }, null);

        return future;
    }

    @Override
    public void deleteComplete(Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Deleted message at {}", replicatorId, ctx);
        }
    }

    @Override
    public void deleteFailed(ManagedLedgerException exception, Object ctx) {
        log.error("[{}] Failed to delete message at {}: {}", replicatorId, ctx,
                exception.getMessage(), exception);
        if (ctx instanceof PositionImpl) {
            PositionImpl deletedEntry = (PositionImpl) ctx;
            if (deletedEntry.compareTo((PositionImpl) cursor.getMarkDeletedPosition()) > 0) {
                brokerService.getPulsar().getExecutor().schedule(
                        () -> cursor.asyncDelete(deletedEntry, (PersistentReplicator) this, deletedEntry), 10,
                        TimeUnit.SECONDS);
            }
        }
    }

    public void updateRates() {
        msgOut.calculateRate();
        msgExpired.calculateRate();
        stats.msgRateOut = msgOut.getRate();
        stats.msgThroughputOut = msgOut.getValueRate();
        stats.msgRateExpired = msgExpired.getRate() + expiryMonitor.getMessageExpiryRate();
    }

    public ReplicatorStatsImpl getStats() {
        stats.replicationBacklog = cursor != null ? cursor.getNumberOfEntriesInBacklog(false) : 0;
        stats.connected = producer != null && producer.isConnected();
        stats.replicationDelayInSeconds = getReplicationDelayInSeconds();

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

    private long getReplicationDelayInSeconds() {
        if (producer != null) {
            return TimeUnit.MILLISECONDS.toSeconds(producer.getDelayInMillis());
        }
        return 0L;
    }

    public boolean expireMessages(int messageTTLInSeconds) {
        if ((cursor.getNumberOfEntriesInBacklog(false) == 0)
                || (cursor.getNumberOfEntriesInBacklog(false) < MINIMUM_BACKLOG_FOR_EXPIRY_CHECK
                        && !topic.isOldestMessageExpired(cursor, messageTTLInSeconds))) {
            // don't do anything for almost caught-up connected subscriptions
            return false;
        }

        return expiryMonitor.expireMessages(messageTTLInSeconds);
    }

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
                this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(topic, Type.REPLICATOR));
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
    public CompletableFuture<Void> disconnect() {
        return disconnect(false);
    }

    @Override
    public synchronized CompletableFuture<Void> disconnect(boolean failIfHasBacklog) {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        super.disconnect(failIfHasBacklog).thenRun(() -> {
            dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);
            future.complete(null);
        }).exceptionally(ex -> {
            Throwable t = (ex instanceof CompletionException ? ex.getCause() : ex);
            if (!(t instanceof TopicBusyException)) {
                log.error("[{}] Failed to close dispatch rate limiter: {}", replicatorId, ex.getMessage());
            }
            future.completeExceptionally(t);
            return null;
        });

        return future;
    }

    @Override
    public boolean isConnected() {
        ProducerImpl<?> producer = this.producer;
        return producer != null && producer.isConnected();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentReplicator.class);

    @VisibleForTesting
    public ManagedCursor getCursor() {
        return cursor;
    }
}
