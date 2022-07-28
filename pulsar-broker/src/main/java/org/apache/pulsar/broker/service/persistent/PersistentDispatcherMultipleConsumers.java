/**
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
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTracker;
import org.apache.pulsar.broker.service.AbstractDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.EntryWrapper;
import org.apache.pulsar.broker.service.InMemoryRedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTrackerDisabled;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.Type;
import org.apache.pulsar.broker.transaction.exception.buffer.TransactionBufferException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PersistentDispatcherMultipleConsumers extends AbstractDispatcherMultipleConsumers
        implements Dispatcher, ReadEntriesCallback {

    protected final PersistentTopic topic;
    protected final ManagedCursor cursor;
    protected volatile Range<PositionImpl> lastIndividualDeletedRangeFromCursorRecovery;

    private CompletableFuture<Void> closeFuture = null;
    protected final MessageRedeliveryController redeliveryMessages;
    protected final RedeliveryTracker redeliveryTracker;

    private Optional<DelayedDeliveryTracker> delayedDeliveryTracker = Optional.empty();

    protected volatile boolean havePendingRead = false;
    protected volatile boolean havePendingReplayRead = false;
    protected volatile PositionImpl minReplayedPosition = null;
    protected boolean shouldRewindBeforeReadingOrReplaying = false;
    protected final String name;
    protected boolean sendInProgress;
    protected static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers>
            TOTAL_AVAILABLE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class,
                    "totalAvailablePermits");
    protected volatile int totalAvailablePermits = 0;
    protected volatile int readBatchSize;
    protected final Backoff readFailureBackoff;
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers>
            TOTAL_UNACKED_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class,
                    "totalUnackedMessages");
    protected volatile int totalUnackedMessages = 0;
    private volatile int blockedDispatcherOnUnackedMsgs = FALSE;
    protected static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers>
            BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class,
                    "blockedDispatcherOnUnackedMsgs");
    protected Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();

    private AtomicBoolean isRescheduleReadInProgress = new AtomicBoolean(false);

    protected enum ReadType {
        Normal, Replay
    }

    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
            Subscription subscription) {
        this(topic, cursor, subscription, true);
    }

    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor, Subscription subscription,
            boolean allowOutOfOrderDelivery) {
        super(subscription, topic.getBrokerService().pulsar().getConfiguration());
        this.cursor = cursor;
        this.lastIndividualDeletedRangeFromCursorRecovery = cursor.getLastIndividualDeletedRange();
        this.name = topic.getName() + " / " + Codec.decode(cursor.getName());
        this.topic = topic;
        this.redeliveryMessages = new MessageRedeliveryController(allowOutOfOrderDelivery);
        this.redeliveryTracker = this.serviceConfig.isSubscriptionRedeliveryTrackerEnabled()
                ? new InMemoryRedeliveryTracker()
                : RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
        this.readBatchSize = serviceConfig.getDispatcherMaxReadBatchSize();
        this.initializeDispatchRateLimiterIfNeeded();
        this.readFailureBackoff = new Backoff(
                topic.getBrokerService().pulsar().getConfiguration().getDispatcherReadFailureBackoffInitialTimeInMs(),
                TimeUnit.MILLISECONDS,
                1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer {}", name, consumer);
            consumer.disconnect();
            return;
        }
        if (consumerList.isEmpty()) {
            if (havePendingRead || havePendingReplayRead) {
                // There is a pending read from previous run. We must wait for it to complete and then rewind
                shouldRewindBeforeReadingOrReplaying = true;
            } else {
                cursor.rewind();
                shouldRewindBeforeReadingOrReplaying = false;
            }
            redeliveryMessages.clear();
            delayedDeliveryTracker.ifPresent(DelayedDeliveryTracker::clear);
        }

        if (isConsumersExceededOnSubscription()) {
            log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit", name);
            throw new ConsumerBusyException("Subscription reached max consumers limit");
        }

        consumerList.add(consumer);
        if (consumerList.size() > 1
                && consumer.getPriorityLevel() < consumerList.get(consumerList.size() - 2).getPriorityLevel()) {
            consumerList.sort(Comparator.comparingInt(Consumer::getPriorityLevel));
        }
        consumerSet.add(consumer);
    }

    @Override
    protected boolean isConsumersExceededOnSubscription() {
        return isConsumersExceededOnSubscription(topic, consumerList.size());
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        // decrement unack-message count for removed consumer
        addUnAckedMessages(-consumer.getUnackedMessages());
        if (consumerSet.removeAll(consumer) == 1) {
            consumerList.remove(consumer);
            log.info("Removed consumer {} with pending {} acks", consumer, consumer.getPendingAcks().size());
            if (consumerList.isEmpty()) {
                cancelPendingRead();

                redeliveryMessages.clear();
                redeliveryTracker.clear();
                if (closeFuture != null) {
                    log.info("[{}] All consumers removed. Subscription is disconnected", name);
                    closeFuture.complete(null);
                }
                totalAvailablePermits = 0;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Consumer are left, reading more entries", name);
                }
                consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> {
                    if (addMessageToReplay(ledgerId, entryId, stickyKeyHash)) {
                        redeliveryTracker.addIfAbsent(PositionImpl.get(ledgerId, entryId));
                    }
                });
                totalAvailablePermits -= consumer.getAvailablePermits();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Decreased totalAvailablePermits by {} in PersistentDispatcherMultipleConsumers. "
                                    + "New dispatcher permit count is {}", name, consumer.getAvailablePermits(),
                            totalAvailablePermits);
                }
                readMoreEntries();
            }
        } else {
            log.info("[{}] Trying to remove a non-connected consumer: {}", name, consumer);
        }
    }

    @Override
    public synchronized void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        if (!consumerSet.contains(consumer)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring flow control from disconnected consumer {}", name, consumer);
            }
            return;
        }

        totalAvailablePermits += additionalNumberOfMessages;

        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Trigger new read after receiving flow control message with permits {} "
                            + "after adding {} permits", name, consumer,
                    totalAvailablePermits, additionalNumberOfMessages);
        }
        readMoreEntries();
    }

    public synchronized void readMoreEntries() {
        if (sendInProgress) {
            // we cannot read more entries while sending the previous batch
            // otherwise we could re-read the same entries and send duplicates
            return;
        }
        if (shouldPauseDeliveryForDelayTracker()) {
            return;
        }

        // totalAvailablePermits may be updated by other threads
        int firstAvailableConsumerPermits = getFirstAvailableConsumerPermits();
        int currentTotalAvailablePermits = Math.max(totalAvailablePermits, firstAvailableConsumerPermits);
        if (currentTotalAvailablePermits > 0 && firstAvailableConsumerPermits > 0) {
            Pair<Integer, Long> calculateResult = calculateToRead(currentTotalAvailablePermits);
            int messagesToRead = calculateResult.getLeft();
            long bytesToRead = calculateResult.getRight();

            if (messagesToRead == -1 || bytesToRead == -1) {
                // Skip read as topic/dispatcher has exceed the dispatch rate or previous pending read hasn't complete.
                return;
            }

            NavigableSet<PositionImpl> messagesToReplayNow = getMessagesToReplayNow(messagesToRead);

            if (!messagesToReplayNow.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule replay of {} messages for {} consumers", name, messagesToReplayNow.size(),
                            consumerList.size());
                }

                havePendingReplayRead = true;
                minReplayedPosition = messagesToReplayNow.first();
                Set<? extends Position> deletedMessages = topic.isDelayedDeliveryEnabled()
                        ? asyncReplayEntriesInOrder(messagesToReplayNow) : asyncReplayEntries(messagesToReplayNow);
                // clear already acked positions from replay bucket

                deletedMessages.forEach(position -> redeliveryMessages.remove(((PositionImpl) position).getLedgerId(),
                        ((PositionImpl) position).getEntryId()));
                // if all the entries are acked-entries and cleared up from redeliveryMessages, try to read
                // next entries as readCompletedEntries-callback was never called
                if ((messagesToReplayNow.size() - deletedMessages.size()) == 0) {
                    havePendingReplayRead = false;
                    // We should not call readMoreEntries() recursively in the same thread
                    // as there is a risk of StackOverflowError
                    topic.getBrokerService().executor().execute(() -> readMoreEntries());
                }
            } else if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE) {
                log.warn("[{}] Dispatcher read is blocked due to unackMessages {} reached to max {}", name,
                        totalUnackedMessages, topic.getMaxUnackedMessagesOnSubscription());
            } else if (!havePendingRead) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule read of {} messages for {} consumers", name, messagesToRead,
                            consumerList.size());
                }
                havePendingRead = true;
                NavigableSet<PositionImpl> toReplay = getMessagesToReplayNow(1);
                if (!toReplay.isEmpty()) {
                    minReplayedPosition = toReplay.first();
                    redeliveryMessages.add(minReplayedPosition.getLedgerId(), minReplayedPosition.getEntryId());
                } else {
                    minReplayedPosition = null;
                }

                cursor.asyncReadEntriesOrWait(messagesToRead, bytesToRead, this,
                        ReadType.Normal, topic.getMaxReadPosition());
            } else {
                log.debug("[{}] Cannot schedule next read until previous one is done", name);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer buffer is full, pause reading", name);
            }
        }
    }

    @Override
    protected void reScheduleRead() {
        if (isRescheduleReadInProgress.compareAndSet(false, true)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Reschedule message read in {} ms", topic.getName(), name, MESSAGE_RATE_BACKOFF_MS);
            }
            topic.getBrokerService().executor().schedule(
                    () -> {
                        isRescheduleReadInProgress.set(false);
                        readMoreEntries();
                        },
                    MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
    }

    // left pair is messagesToRead, right pair is bytesToRead
    protected Pair<Integer, Long> calculateToRead(int currentTotalAvailablePermits) {
        int messagesToRead = Math.min(currentTotalAvailablePermits, readBatchSize);
        long bytesToRead = serviceConfig.getDispatcherMaxReadSizeBytes();

        Consumer c = getRandomConsumer();
        // if turn on precise dispatcher flow control, adjust the record to read
        if (c != null && c.isPreciseDispatcherFlowControl()) {
            int avgMessagesPerEntry = Math.max(1, c.getAvgMessagesPerEntry());
            messagesToRead = Math.min(
                    (int) Math.ceil(currentTotalAvailablePermits * 1.0 / avgMessagesPerEntry),
                    readBatchSize);
        }

        if (!isConsumerWritable()) {
            // If the connection is not currently writable, we issue the read request anyway, but for a single
            // message. The intent here is to keep use the request as a notification mechanism while avoiding to
            // read and dispatch a big batch of messages which will need to wait before getting written to the
            // socket.
            messagesToRead = 1;
        }

        // throttle only if: (1) cursor is not active (or flag for throttle-nonBacklogConsumer is enabled) bcz
        // active-cursor reads message from cache rather from bookkeeper (2) if topic has reached message-rate
        // threshold: then schedule the read after MESSAGE_RATE_BACKOFF_MS
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            if (topic.getBrokerDispatchRateLimiter().isPresent()) {
                DispatchRateLimiter brokerRateLimiter = topic.getBrokerDispatchRateLimiter().get();
                if (reachDispatchRateLimit(brokerRateLimiter)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded broker message-rate {}/{}, schedule after a {}", name,
                                brokerRateLimiter.getDispatchRateOnMsg(), brokerRateLimiter.getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    return Pair.of(-1, -1L);
                } else {
                    Pair<Integer, Long> calculateToRead =
                            updateMessagesToRead(brokerRateLimiter, messagesToRead, bytesToRead);
                    messagesToRead = calculateToRead.getLeft();
                    bytesToRead = calculateToRead.getRight();
                }
            }

            if (topic.getDispatchRateLimiter().isPresent()) {
                DispatchRateLimiter topicRateLimiter = topic.getDispatchRateLimiter().get();
                if (reachDispatchRateLimit(topicRateLimiter)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded topic message-rate {}/{}, schedule after a {}", name,
                                topicRateLimiter.getDispatchRateOnMsg(), topicRateLimiter.getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    return Pair.of(-1, -1L);
                } else {
                    Pair<Integer, Long> calculateToRead =
                            updateMessagesToRead(topicRateLimiter, messagesToRead, bytesToRead);
                    messagesToRead = calculateToRead.getLeft();
                    bytesToRead = calculateToRead.getRight();
                }
            }

            if (dispatchRateLimiter.isPresent()) {
                if (reachDispatchRateLimit(dispatchRateLimiter.get())) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded subscription message-rate {}/{}, schedule after a {}",
                                name, dispatchRateLimiter.get().getDispatchRateOnMsg(),
                                dispatchRateLimiter.get().getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    return Pair.of(-1, -1L);
                } else {
                    Pair<Integer, Long> calculateToRead =
                            updateMessagesToRead(dispatchRateLimiter.get(), messagesToRead, bytesToRead);
                    messagesToRead = calculateToRead.getLeft();
                    bytesToRead = calculateToRead.getRight();
                }
            }
        }

        if (havePendingReplayRead) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Skipping replay while awaiting previous read to complete", name);
            }
            return Pair.of(-1, -1L);
        }

        // If messagesToRead is 0 or less, correct it to 1 to prevent IllegalArgumentException
        messagesToRead = Math.max(messagesToRead, 1);
        bytesToRead = Math.max(bytesToRead, 1);
        return Pair.of(messagesToRead, bytesToRead);
    }

    protected Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay);
    }

    protected Set<? extends Position> asyncReplayEntriesInOrder(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay, true);
    }

    @Override
    public boolean isConsumerConnected() {
        return !consumerList.isEmpty();
    }

    @Override
    public CopyOnWriteArrayList<Consumer> getConsumers() {
        return consumerList;
    }

    @Override
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return consumerList.size() == 1 && consumerSet.contains(consumer);
    }

    @Override
    public CompletableFuture<Void> close() {
        IS_CLOSED_UPDATER.set(this, TRUE);

        Optional<DelayedDeliveryTracker> delayedDeliveryTracker;
        synchronized (this) {
            delayedDeliveryTracker = this.delayedDeliveryTracker;
            this.delayedDeliveryTracker = Optional.empty();
        }

        delayedDeliveryTracker.ifPresent(DelayedDeliveryTracker::close);

        dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

        return disconnectAllConsumers();
    }

    @Override
    public synchronized CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
        closeFuture = new CompletableFuture<>();
        if (consumerList.isEmpty()) {
            closeFuture.complete(null);
        } else {
            consumerList.forEach(consumer -> consumer.disconnect(isResetCursor));
            cancelPendingRead();
        }
        return closeFuture;
    }

    @Override
    protected void cancelPendingRead() {
        if (havePendingRead && cursor.cancelPendingReadRequest()) {
            havePendingRead = false;
        }
    }

    @Override
    public CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
        return disconnectAllConsumers(isResetCursor);
    }

    @Override
    public synchronized void resetCloseFuture() {
        closeFuture = null;
    }

    @Override
    public void reset() {
        resetCloseFuture();
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    @Override
    public SubType getType() {
        return SubType.Shared;
    }

    @Override
    public final synchronized void readEntriesComplete(List<Entry> entries, Object ctx) {
        ReadType readType = (ReadType) ctx;
        if (readType == ReadType.Normal) {
            havePendingRead = false;
        } else {
            havePendingReplayRead = false;
        }

        if (readBatchSize < serviceConfig.getDispatcherMaxReadBatchSize()) {
            int newReadBatchSize = Math.min(readBatchSize * 2, serviceConfig.getDispatcherMaxReadBatchSize());
            if (log.isDebugEnabled()) {
                log.debug("[{}] Increasing read batch size from {} to {}", name, readBatchSize, newReadBatchSize);
            }

            readBatchSize = newReadBatchSize;
        }

        readFailureBackoff.reduceToHalf();

        if (shouldRewindBeforeReadingOrReplaying && readType == ReadType.Normal) {
            // All consumers got disconnected before the completion of the read operation
            entries.forEach(Entry::release);
            cursor.rewind();
            shouldRewindBeforeReadingOrReplaying = false;
            readMoreEntries();
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Distributing {} messages to {} consumers", name, entries.size(), consumerList.size());
        }

        sendMessagesToConsumers(readType, entries);
    }

    protected final synchronized void sendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        sendInProgress = true;
        boolean readMoreEntries;
        try {
            readMoreEntries = trySendMessagesToConsumers(readType, entries);
        } finally {
            sendInProgress = false;
        }
        if (readMoreEntries) {
            readMoreEntries();
        }
    }

    /**
     * Dispatch the messages to the Consumers.
     * @return true if you want to trigger a new read.
     * This method is overridden by other classes, please take a look to other implementations
     * if you need to change it.
     */
    protected synchronized boolean trySendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        if (needTrimAckedMessages()) {
            cursor.trimDeletedEntries(entries);
        }

        int entriesToDispatch = entries.size();
        // Trigger read more messages
        if (entriesToDispatch == 0) {
            return true;
        }
        EntryWrapper[] entryWrappers = new EntryWrapper[entries.size()];
        int remainingMessages = updateEntryWrapperWithMetadata(entryWrappers, entries);
        int start = 0;
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        long totalEntries = 0;
        int avgBatchSizePerMsg = remainingMessages > 0 ? Math.max(remainingMessages / entries.size(), 1) : 1;

        int firstAvailableConsumerPermits, currentTotalAvailablePermits;
        boolean dispatchMessage;
        while (entriesToDispatch > 0) {
            firstAvailableConsumerPermits = getFirstAvailableConsumerPermits();
            currentTotalAvailablePermits = Math.max(totalAvailablePermits, firstAvailableConsumerPermits);
            dispatchMessage = currentTotalAvailablePermits > 0 && firstAvailableConsumerPermits > 0;
            if (!dispatchMessage) {
                break;
            }
            Consumer c = getNextConsumer();
            if (c == null) {
                // Do nothing, cursor will be rewind at reconnection
                log.info("[{}] rewind because no available consumer found from total {}", name, consumerList.size());
                entries.subList(start, entries.size()).forEach(Entry::release);
                cursor.rewind();
                return false;
            }

            // round-robin dispatch batch size for this consumer
            int availablePermits = c.isWritable() ? c.getAvailablePermits() : 1;
            if (c.getMaxUnackedMessages() > 0) {
                // Avoid negative number
                int remainUnAckedMessages = Math.max(c.getMaxUnackedMessages() - c.getUnackedMessages(), 0);
                availablePermits = Math.min(availablePermits, remainUnAckedMessages);
            }
            if (log.isDebugEnabled() && !c.isWritable()) {
                log.debug("[{}-{}] consumer is not writable. dispatching only 1 message to {}; "
                                + "availablePermits are {}", topic.getName(), name,
                        c, c.getAvailablePermits());
            }

            int messagesForC = Math.min(Math.min(remainingMessages, availablePermits),
                    serviceConfig.getDispatcherMaxRoundRobinBatchSize());
            messagesForC = Math.max(messagesForC / avgBatchSizePerMsg, 1);

            if (messagesForC > 0) {
                int end = Math.min(start + messagesForC, entries.size());
                // remove positions first from replay list first : sendMessages recycles entries
                if (readType == ReadType.Replay) {
                    entries.subList(start, end).forEach(entry -> {
                        redeliveryMessages.remove(entry.getLedgerId(), entry.getEntryId());
                    });
                }

                SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                List<Entry> entriesForThisConsumer = entries.subList(start, end);

                EntryBatchSizes batchSizes = EntryBatchSizes.get(entriesForThisConsumer.size());
                EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entriesForThisConsumer.size());
                totalEntries += filterEntriesForConsumer(entryWrappers, start,
                        entriesForThisConsumer, batchSizes, sendMessageInfo, batchIndexesAcks, cursor,
                        readType == ReadType.Replay, c);

                c.sendMessages(entriesForThisConsumer, batchSizes, batchIndexesAcks, sendMessageInfo.getTotalMessages(),
                        sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(), redeliveryTracker);

                int msgSent = sendMessageInfo.getTotalMessages();
                remainingMessages -= msgSent;
                start += messagesForC;
                entriesToDispatch -= messagesForC;
                TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this,
                        -(msgSent - batchIndexesAcks.getTotalAckedIndexCount()));
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Added -({} minus {}) permits to TOTAL_AVAILABLE_PERMITS_UPDATER in "
                                    + "PersistentDispatcherMultipleConsumers",
                            name, msgSent, batchIndexesAcks.getTotalAckedIndexCount());
                }
                totalMessagesSent += sendMessageInfo.getTotalMessages();
                totalBytesSent += sendMessageInfo.getTotalBytes();
            }
        }

        // release entry-wrapper
        for (EntryWrapper entry : entryWrappers) {
            if (entry != null) {
                entry.recycle();
            }
        }

        // acquire message-dispatch permits for already delivered messages
        long permits = dispatchThrottlingOnBatchMessageEnabled ? totalEntries : totalMessagesSent;
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            if (topic.getBrokerDispatchRateLimiter().isPresent()) {
                topic.getBrokerDispatchRateLimiter().get().tryDispatchPermit(permits, totalBytesSent);
            }
            if (topic.getDispatchRateLimiter().isPresent()) {
                topic.getDispatchRateLimiter().get().tryDispatchPermit(permits, totalBytesSent);
            }

            if (dispatchRateLimiter.isPresent()) {
                dispatchRateLimiter.get().tryDispatchPermit(permits, totalBytesSent);
            }
        }

        if (entriesToDispatch > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No consumers found with available permits, storing {} positions for later replay", name,
                        entries.size() - start);
            }
            entries.subList(start, entries.size()).forEach(entry -> {
                long stickyKeyHash = getStickyKeyHash(entry);
                addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                entry.release();
            });
        }
        return true;
    }

    @Override
    public synchronized void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

        ReadType readType = (ReadType) ctx;
        long waitTimeMillis = readFailureBackoff.next();

        if (exception instanceof NoMoreEntriesToReadException) {
            if (cursor.getNumberOfEntriesInBacklog(false) == 0) {
                // Topic has been terminated and there are no more entries to read
                // Notify the consumer only if all the messages were already acknowledged
                consumerList.forEach(Consumer::reachedEndOfTopic);
            }
        } else if (exception.getCause() instanceof TransactionBufferException.TransactionNotSealedException
                || exception.getCause() instanceof ManagedLedgerException.OffloadReadHandleClosedException) {
            waitTimeMillis = 1;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error reading transaction entries : {}, Read Type {} - Retrying to read in {} seconds",
                        name, exception.getMessage(), readType, waitTimeMillis / 1000.0);
            }
        } else if (!(exception instanceof TooManyRequestsException)) {
            log.error("[{}] Error reading entries at {} : {}, Read Type {} - Retrying to read in {} seconds", name,
                    cursor.getReadPosition(), exception.getMessage(), readType, waitTimeMillis / 1000.0);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error reading entries at {} : {}, Read Type {} - Retrying to read in {} seconds", name,
                        cursor.getReadPosition(), exception.getMessage(), readType, waitTimeMillis / 1000.0);
            }
        }

        if (shouldRewindBeforeReadingOrReplaying) {
            shouldRewindBeforeReadingOrReplaying = false;
            cursor.rewind();
        }

        if (readType == ReadType.Normal) {
            havePendingRead = false;
        } else {
            havePendingReplayRead = false;
            if (exception instanceof ManagedLedgerException.InvalidReplayPositionException) {
                PositionImpl markDeletePosition = (PositionImpl) cursor.getMarkDeletedPosition();
                redeliveryMessages.removeAllUpTo(markDeletePosition.getLedgerId(), markDeletePosition.getEntryId());
            }
        }

        readBatchSize = serviceConfig.getDispatcherMinReadBatchSize();

        topic.getBrokerService().executor().schedule(() -> {
            synchronized (PersistentDispatcherMultipleConsumers.this) {
                // If it's a replay read we need to retry even if there's already
                // another scheduled read, otherwise we'd be stuck until
                // more messages are published.
                if (!havePendingRead || readType == ReadType.Replay) {
                    log.info("[{}] Retrying read operation", name);
                    readMoreEntries();
                } else {
                    log.info("[{}] Skipping read retry: havePendingRead {}", name, havePendingRead, exception);
                }
            }
        }, waitTimeMillis, TimeUnit.MILLISECONDS);

    }

    private boolean needTrimAckedMessages() {
        if (lastIndividualDeletedRangeFromCursorRecovery == null) {
            return false;
        } else {
            return lastIndividualDeletedRangeFromCursorRecovery.upperEndpoint()
                    .compareTo((PositionImpl) cursor.getReadPosition()) > 0;
        }
    }

    /**
     * returns true only if {@link AbstractDispatcherMultipleConsumers#consumerList}
     * has atleast one unblocked consumer and have available permits.
     *
     * @return
     */
    protected boolean isAtleastOneConsumerAvailable() {
        return getFirstAvailableConsumerPermits() > 0;
    }

    protected int getFirstAvailableConsumerPermits() {
        if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
            // abort read if no consumers are connected or if disconnect is initiated
            return 0;
        }
        for (Consumer consumer : consumerList) {
            if (consumer != null && !consumer.isBlocked()) {
                int availablePermits = consumer.getAvailablePermits();
                if (availablePermits > 0) {
                    return availablePermits;
                }
            }
        }
        return 0;
    }

    private boolean isConsumerWritable() {
        for (Consumer consumer : consumerList) {
            if (consumer.isWritable()) {
                return true;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] consumer is not writable", topic.getName(), name);
        }
        return false;
    }

    @Override
    public boolean isConsumerAvailable(Consumer consumer) {
        return consumer != null && !consumer.isBlocked() && consumer.getAvailablePermits() > 0;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {
        consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> {
            addMessageToReplay(ledgerId, entryId, stickyKeyHash);
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer,
                    redeliveryMessages);
        }
        readMoreEntries();
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        positions.forEach(position -> {
            // TODO: We want to pass a sticky key hash as a third argument to guarantee the order of the messages
            // on Key_Shared subscription, but it's difficult to get the sticky key here
            if (addMessageToReplay(position.getLedgerId(), position.getEntryId())) {
                redeliveryTracker.addIfAbsent(position);
            }
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer, positions);
        }
        readMoreEntries();
    }

    @Override
    public void addUnAckedMessages(int numberOfMessages) {
        int maxUnackedMessages = topic.getMaxUnackedMessagesOnSubscription();
        // don't block dispatching if maxUnackedMessages = 0
        if (maxUnackedMessages <= 0 && blockedDispatcherOnUnackedMsgs == TRUE
                && BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
            log.info("[{}] Dispatcher is unblocked, since maxUnackedMessagesPerSubscription=0", name);
            topic.getBrokerService().executor().execute(() -> readMoreEntries());
        }

        int unAckedMessages = TOTAL_UNACKED_MESSAGES_UPDATER.addAndGet(this, numberOfMessages);
        if (unAckedMessages >= maxUnackedMessages && maxUnackedMessages > 0
                && BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, FALSE, TRUE)) {
            // block dispatcher if it reaches maxUnAckMsg limit
            log.info("[{}] Dispatcher is blocked due to unackMessages {} reached to max {}", name,
                    TOTAL_UNACKED_MESSAGES_UPDATER.get(this), maxUnackedMessages);
        } else if (topic.getBrokerService().isBrokerDispatchingBlocked()
                && blockedDispatcherOnUnackedMsgs == TRUE) {
            // unblock dispatcher: if dispatcher is blocked due to broker-unackMsg limit and if it ack back enough
            // messages
            if (totalUnackedMessages < (topic.getBrokerService().maxUnackedMsgsPerDispatcher / 2)) {
                if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                    // it removes dispatcher from blocked list and unblocks dispatcher by scheduling read
                    topic.getBrokerService().unblockDispatchersOnUnAckMessages(Lists.newArrayList(this));
                }
            }
        } else if (blockedDispatcherOnUnackedMsgs == TRUE && unAckedMessages < maxUnackedMessages / 2) {
            // unblock dispatcher if it acks back enough messages
            if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                log.info("[{}] Dispatcher is unblocked", name);
                topic.getBrokerService().executor().execute(() -> readMoreEntries());
            }
        }
        // increment broker-level count
        topic.getBrokerService().addUnAckedMessages(this, numberOfMessages);
    }

    public boolean isBlockedDispatcherOnUnackedMsgs() {
        return blockedDispatcherOnUnackedMsgs == TRUE;
    }

    public void blockDispatcherOnUnackedMsgs() {
        blockedDispatcherOnUnackedMsgs = TRUE;
    }

    public void unBlockDispatcherOnUnackedMsgs() {
        blockedDispatcherOnUnackedMsgs = FALSE;
    }

    public int getTotalUnackedMessages() {
        return totalUnackedMessages;
    }

    public String getName() {
        return name;
    }

    @Override
    public RedeliveryTracker getRedeliveryTracker() {
        return redeliveryTracker;
    }

    @Override
    public Optional<DispatchRateLimiter> getRateLimiter() {
        return dispatchRateLimiter;
    }

    @Override
    public void updateRateLimiter() {
        if (!initializeDispatchRateLimiterIfNeeded()) {
            this.dispatchRateLimiter.ifPresent(DispatchRateLimiter::updateDispatchRate);
        }
    }

    @Override
    public boolean initializeDispatchRateLimiterIfNeeded() {
        if (!dispatchRateLimiter.isPresent()
            && DispatchRateLimiter.isDispatchRateEnabled(topic.getSubscriptionDispatchRate())) {
            this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(topic, Type.SUBSCRIPTION));
            return true;
        }
        return false;
    }

    @Override
    public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
        if (!topic.isDelayedDeliveryEnabled()) {
            // If broker has the feature disabled, always deliver messages immediately
            return false;
        }

        synchronized (this) {
            if (!delayedDeliveryTracker.isPresent()) {
                if (!msgMetadata.hasDeliverAtTime()) {
                    // No need to initialize the tracker here
                    return false;
                }

                // Initialize the tracker the first time we need to use it
                delayedDeliveryTracker = Optional
                        .of(topic.getBrokerService().getDelayedDeliveryTrackerFactory().newTracker(this));
            }

            delayedDeliveryTracker.get().resetTickTime(topic.getDelayedDeliveryTickTimeMillis());

            long deliverAtTime = msgMetadata.hasDeliverAtTime() ? msgMetadata.getDeliverAtTime() : -1L;
            return delayedDeliveryTracker.get().addMessage(ledgerId, entryId, deliverAtTime);
        }
    }

    protected synchronized NavigableSet<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        if (delayedDeliveryTracker.isPresent() && delayedDeliveryTracker.get().hasMessageAvailable()) {
            delayedDeliveryTracker.get().resetTickTime(topic.getDelayedDeliveryTickTimeMillis());
            NavigableSet<PositionImpl> messagesAvailableNow =
                    delayedDeliveryTracker.get().getScheduledMessages(maxMessagesToRead);
            messagesAvailableNow.forEach(p -> redeliveryMessages.add(p.getLedgerId(), p.getEntryId()));
        }

        if (!redeliveryMessages.isEmpty()) {
            return redeliveryMessages.getMessagesToReplayNow(maxMessagesToRead);
        } else {
            return Collections.emptyNavigableSet();
        }
    }

    protected synchronized boolean shouldPauseDeliveryForDelayTracker() {
        return delayedDeliveryTracker.isPresent() && delayedDeliveryTracker.get().shouldPauseAllDeliveries();
    }

    @Override
    public synchronized long getNumberOfDelayedMessages() {
        return delayedDeliveryTracker.map(DelayedDeliveryTracker::getNumberOfDelayedMessages).orElse(0L);
    }

    @Override
    public void clearDelayedMessages() {
        this.delayedDeliveryTracker.ifPresent(DelayedDeliveryTracker::clear);
    }

    @Override
    public void cursorIsReset() {
        if (this.lastIndividualDeletedRangeFromCursorRecovery != null) {
            this.lastIndividualDeletedRangeFromCursorRecovery = null;
        }
    }

    protected boolean addMessageToReplay(long ledgerId, long entryId, long stickyKeyHash) {
        if (checkIfMessageIsUnacked(ledgerId, entryId)) {
            redeliveryMessages.add(ledgerId, entryId, stickyKeyHash);
            return true;
        } else {
            return false;
        }
    }

    protected boolean addMessageToReplay(long ledgerId, long entryId) {
        if (checkIfMessageIsUnacked(ledgerId, entryId)) {
            redeliveryMessages.add(ledgerId, entryId);
            return true;
        } else {
            return false;
        }
    }

    private boolean checkIfMessageIsUnacked(long ledgerId, long entryId) {
        Position markDeletePosition = cursor.getMarkDeletedPosition();
        return (markDeletePosition == null || ledgerId > markDeletePosition.getLedgerId()
                || (ledgerId == markDeletePosition.getLedgerId() && entryId > markDeletePosition.getEntryId()));
    }

    @Override
    public boolean checkAndUnblockIfStuck() {
        if (cursor.checkAndUpdateReadPositionChanged()) {
            return false;
        }
        // consider dispatch is stuck if : dispatcher has backlog, available-permits and there is no pending read
        if (totalAvailablePermits > 0 && !havePendingReplayRead && !havePendingRead
                && cursor.getNumberOfEntriesInBacklog(false) > 0) {
            log.warn("{}-{} Dispatcher is stuck and unblocking by issuing reads", topic.getName(), name);
            readMoreEntries();
            return true;
        }
        return false;
    }

    public PersistentTopic getTopic() {
        return topic;
    }

    protected int getStickyKeyHash(Entry entry) {
        return StickyKeyConsumerSelector.makeStickyKeyHash(peekStickyKey(entry.getDataBuffer()));
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherMultipleConsumers.class);
}
