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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import static java.util.stream.Collectors.toSet;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet;
import org.apache.pulsar.utils.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

/**
 */
public class PersistentDispatcherMultipleConsumers implements Dispatcher, ReadEntriesCallback {

    private static final int MaxReadBatchSize = 100;
    private static final int MaxRoundRobinBatchSize = 20;

    private final PersistentTopic topic;
    private final ManagedCursor cursor;
    private final CopyOnWriteArrayList<Consumer> consumerList = new CopyOnWriteArrayList<>();
    private final ObjectSet<Consumer> consumerSet = new ObjectHashSet<>();

    private CompletableFuture<Void> closeFuture = null;
    private ConcurrentLongPairSet messagesToReplay;

    private int currentConsumerRoundRobinIndex = 0;
    private boolean havePendingRead = false;
    private boolean havePendingReplayRead = false;
    private boolean shouldRewindBeforeReadingOrReplaying = false;
    private final String name;

    private int totalAvailablePermits = 0;
    private int readBatchSize;
    private final Backoff readFailureBackoff = new Backoff(15, TimeUnit.SECONDS, 1, TimeUnit.MINUTES);
    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> IS_CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "isClosed");
    private volatile int isClosed = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> TOTAL_UNACKED_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "totalUnackedMessages");
    private volatile int totalUnackedMessages = 0;
    private final int maxUnackedMessages;
    private volatile int blockedDispatcherOnUnackedMsgs = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "blockedDispatcherOnUnackedMsgs");

    enum ReadType {
        Normal, Replay
    }

    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor) {
        this.cursor = cursor;
        this.name = topic.getName() + " / " + Codec.decode(cursor.getName());
        this.topic = topic;
        this.messagesToReplay = new ConcurrentLongPairSet(512, 2);
        this.readBatchSize = MaxReadBatchSize;
        this.maxUnackedMessages = topic.getBrokerService().pulsar().getConfiguration()
                .getMaxUnackedMessagesPerSubscription();
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer ", name, consumer);
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
            messagesToReplay.clear();
        }

        consumerList.add(consumer);
        consumerList.sort((c1, c2) -> c1.getPriorityLevel() - c2.getPriorityLevel());
        consumerSet.add(consumer);
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        // decrement unack-message count for removed consumer
        addUnAckedMessages(-consumer.getUnackedMessages());
        if (consumerSet.removeAll(consumer) == 1) {
            consumerList.remove(consumer);
            log.info("Removed consumer {} with pending {} acks", consumer, consumer.getPendingAcks().size());
            if (consumerList.isEmpty()) {
                if (havePendingRead && cursor.cancelPendingReadRequest()) {
                    havePendingRead = false;
                }

                messagesToReplay.clear();
                if (closeFuture != null) {
                    log.info("[{}] All consumers removed. Subscription is disconnected", name);
                    closeFuture.complete(null);
                }
                totalAvailablePermits = 0;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Consumer are left, reading more entries", name);
                }
                consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, none) -> {
                    messagesToReplay.add(ledgerId, entryId);
                });
                totalAvailablePermits -= consumer.getAvailablePermits();
                readMoreEntries();
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Trying to remove a non-connected consumer: {}", name, consumer);
            }
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
            log.debug("[{}] Trigger new read after receiving flow control message", consumer);
        }
        readMoreEntries();
    }

    public void readMoreEntries() {
        if (totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            int messagesToRead = Math.min(totalAvailablePermits, readBatchSize);

            if (!messagesToReplay.isEmpty()) {
                if (havePendingReplayRead) {
                    log.debug("[{}] Skipping replay while awaiting previous read to complete", name);
                    return;
                }

                Set<PositionImpl> messagesToReplayNow = messagesToReplay.items(messagesToRead).stream()
                        .map(pair -> new PositionImpl(pair.first, pair.second)).collect(toSet());

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule replay of {} messages for {} consumers", name, messagesToReplayNow.size(),
                            consumerList.size());
                }

                havePendingReplayRead = true;
                Set<? extends Position> deletedMessages = cursor.asyncReplayEntries(messagesToReplayNow, this,
                        ReadType.Replay);
                // clear already acked positions from replay bucket

                deletedMessages.forEach(position -> messagesToReplay.remove(((PositionImpl) position).getLedgerId(),
                        ((PositionImpl) position).getEntryId()));
                // if all the entries are acked-entries and cleared up from messagesToReplay, try to read
                // next entries as readCompletedEntries-callback was never called
                if ((messagesToReplayNow.size() - deletedMessages.size()) == 0) {
                    havePendingReplayRead = false;
                    readMoreEntries();
                }
            } else if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE) {
                log.warn("[{}] Dispatcher read is blocked due to unackMessages {} reached to max {}", name,
                        TOTAL_UNACKED_MESSAGES_UPDATER.get(this), maxUnackedMessages);
            } else if (!havePendingRead) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule read of {} messages for {} consumers", name, messagesToRead,
                            consumerList.size());
                }
                havePendingRead = true;
                cursor.asyncReadEntriesOrWait(messagesToRead, this, ReadType.Normal);
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
        return disconnectAllConsumers();
    }

    @Override
    public synchronized CompletableFuture<Void> disconnectAllConsumers() {
        closeFuture = new CompletableFuture<>();
        if (consumerList.isEmpty()) {
            closeFuture.complete(null);
        } else {
            consumerList.forEach(Consumer::disconnect);
            if (havePendingRead && cursor.cancelPendingReadRequest()) {
                havePendingRead = false;
            }
        }
        return closeFuture;
    }

    @Override
    public void reset() {
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    @Override
    public SubType getType() {
        return SubType.Shared;
    }

    @Override
    public synchronized void readEntriesComplete(List<Entry> entries, Object ctx) {
        ReadType readType = (ReadType) ctx;
        int start = 0;
        int entriesToDispatch = entries.size();

        if (readType == ReadType.Normal) {
            havePendingRead = false;
        } else {
            havePendingReplayRead = false;
        }

        if (readBatchSize < MaxReadBatchSize) {
            int newReadBatchSize = Math.min(readBatchSize * 2, MaxReadBatchSize);
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

        while (entriesToDispatch > 0 && totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            Consumer c = getNextConsumer();
            if (c == null) {
                // Do nothing, cursor will be rewind at reconnection
                entries.subList(start, entries.size()).forEach(Entry::release);
                cursor.rewind();
                return;
            }

            // round-robin dispatch batch size for this consumer
            int messagesForC = Math.min(Math.min(entriesToDispatch, c.getAvailablePermits()), MaxRoundRobinBatchSize);

            if (messagesForC > 0) {

                // remove positions first from replay list first : sendMessages recycles entries
                if (readType == ReadType.Replay) {
                    entries.subList(start, start + messagesForC).forEach(entry -> {
                        messagesToReplay.remove(entry.getLedgerId(), entry.getEntryId());
                    });
                }

                int msgSent = c.sendMessages(entries.subList(start, start + messagesForC)).getRight();

                start += messagesForC;
                entriesToDispatch -= messagesForC;
                totalAvailablePermits -= msgSent;
            }
        }

        if (entriesToDispatch > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No consumers found with available permits, storing {} positions for later replay", name,
                        entries.size() - start);
            }
            entries.subList(start, entries.size()).forEach(entry -> {
                messagesToReplay.add(entry.getLedgerId(), entry.getEntryId());
                entry.release();
            });
        }

        readMoreEntries();
    }

    @Override
    public synchronized void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

        ReadType readType = (ReadType) ctx;
        long waitTimeMillis = readFailureBackoff.next();

        if (exception instanceof NoMoreEntriesToReadException) {
            if (cursor.getNumberOfEntriesInBacklog() == 0) {
                // Topic has been terminated and there are no more entries to read
                // Notify the consumer only if all the messages were already acknowledged
                consumerList.forEach(Consumer::reachedEndOfTopic);
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
                messagesToReplay.removeIf((ledgerId, entryId) -> {
                    return ComparisonChain.start().compare(ledgerId, markDeletePosition.getLedgerId())
                            .compare(entryId, markDeletePosition.getEntryId()).result() <= 0;
                });
            }
        }

        readBatchSize = 1;

        topic.getBrokerService().executor().schedule(() -> {
            synchronized (PersistentDispatcherMultipleConsumers.this) {
                if (!havePendingRead) {
                    log.info("[{}] Retrying read operation", name);
                    readMoreEntries();
                } else {
                    log.info("[{}] Skipping read retry: havePendingRead {}", name, havePendingRead, exception);
                }
            }
        }, waitTimeMillis, TimeUnit.MILLISECONDS);

    }

    /**
     * <pre>
     * Broker gives more priority while dispatching messages. Here, broker follows descending priorities. (eg:
     * 0=max-priority, 1, 2,..)
     * <p>
     * Broker will first dispatch messages to max priority-level consumers if they
     * have permits, else broker will consider next priority level consumers.
     * Also on the same priority-level, it selects consumer in round-robin manner.
     * <p>
     * If subscription has consumer-A with  priorityLevel 1 and Consumer-B with priorityLevel 2 then broker will dispatch
     * messages to only consumer-A until it runs out permit and then broker starts dispatching messages to Consumer-B.
     * <p>
     * Consumer PriorityLevel Permits
     * C1       0             2
     * C2       0             1
     * C3       0             1
     * C4       1             2
     * C5       1             1
     * Result of getNextConsumer(): C1, C2, C3, C1, C4, C5, C4
     * </pre>
     *
     * <pre>
     * <b>Algorithm:</b>
     * 1. consumerList: it stores consumers in sorted-list: max-priority stored first
     * 2. currentConsumerRoundRobinIndex: it always stores last served consumer-index
     *
     * Each time getNextConsumer() is called:<p>
     * 1. It always starts to traverse from the max-priority consumer (first element) from sorted-list
     * 2. Consumers on same priority-level will be treated equally and it tries to pick one of them in round-robin manner
     * 3. If consumer is not available on given priority-level then only it will go to the next lower priority-level consumers
     * 4. Returns null in case it doesn't find any available consumer
     * </pre>
     *
     * @return nextAvailableConsumer
     */
    private Consumer getNextConsumer() {
        if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
            // abort read if no consumers are connected or if disconnect is initiated
            return null;
        }

        if (currentConsumerRoundRobinIndex >= consumerList.size()) {
            currentConsumerRoundRobinIndex = 0;
        }

        int currentRoundRobinConsumerPriority = consumerList.get(currentConsumerRoundRobinIndex).getPriorityLevel();

        // first find available-consumer on higher level unless currentIndex is not on highest level which is 0
        if (currentRoundRobinConsumerPriority != 0) {
            int higherPriorityConsumerIndex = getConsumerFromHigherPriority(currentRoundRobinConsumerPriority);
            if (higherPriorityConsumerIndex != -1) {
                currentConsumerRoundRobinIndex = higherPriorityConsumerIndex + 1;
                return consumerList.get(higherPriorityConsumerIndex);
            }
        }

        // currentIndex is already on highest level or couldn't find consumer on higher level so, find consumer on same or lower
        // level
        int availableConsumerIndex = getNextConsumerFromSameOrLowerLevel(currentConsumerRoundRobinIndex);
        if (availableConsumerIndex != -1) {
            currentConsumerRoundRobinIndex = availableConsumerIndex + 1;
            return consumerList.get(availableConsumerIndex);
        }

        // couldn't find available consumer
        return null;
    }

    /**
     * Finds index of first available consumer which has higher priority then given targetPriority
     * @param targetPriority
     * @return -1 if couldn't find any available consumer
     */
    private int getConsumerFromHigherPriority(int targetPriority) {
        for (int i = 0; i < currentConsumerRoundRobinIndex; i++) {
            Consumer consumer = consumerList.get(i);
            if (consumer.getPriorityLevel() < targetPriority) {
                if (isConsumerAvailable(consumerList.get(i))) {
                    return i;
                }
            } else {
                break;
            }
        }
        return -1;
    }

    /**
     * Finds index of round-robin available consumer that present on same level as consumer on currentRoundRobinIndex if doesn't
     * find consumer on same level then it finds first available consumer on lower priority level else returns index=-1
     * if couldn't find any available consumer in the list
     *
     * @param currentRoundRobinIndex
     * @return
     */
    private int getNextConsumerFromSameOrLowerLevel(int currentRoundRobinIndex) {

        int targetPriority = consumerList.get(currentRoundRobinIndex).getPriorityLevel();
        // use to do round-robin if can't find consumer from currentRR to last-consumer in list
        int scanIndex = currentRoundRobinIndex;
        int endPriorityLevelIndex = currentRoundRobinIndex;
        do {
            Consumer scanConsumer = scanIndex < consumerList.size() ? consumerList.get(scanIndex)
                    : null /* reached to last consumer of list */;

            // if reached to last consumer of list then check from beginning to currentRRIndex of the list
            if (scanConsumer == null || scanConsumer.getPriorityLevel() != targetPriority) {
                endPriorityLevelIndex = scanIndex; // last consumer on this level
                scanIndex = getFirstConsumerIndexOfPriority(targetPriority);
            } else {
                if (isConsumerAvailable(scanConsumer)) {
                    return scanIndex;
                }
                scanIndex++;
            }
        } while (scanIndex != currentRoundRobinIndex);

        // it means: didn't find consumer in the same priority-level so, check available consumer lower than this level
        for (int i = endPriorityLevelIndex; i < consumerList.size(); i++) {
            if (isConsumerAvailable(consumerList.get(i))) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Finds index of first consumer in list which has same priority as given targetPriority
     * @param targetPriority
     * @return
     */
    private int getFirstConsumerIndexOfPriority(int targetPriority) {
        for (int i = 0; i < consumerList.size(); i++) {
            if (consumerList.get(i).getPriorityLevel() == targetPriority) {
                return i;
            }
        }
        return -1;
    }

    /**
     * returns true only if {@link consumerList} has atleast one unblocked consumer and have available permits
     *
     * @return
     */
    private boolean isAtleastOneConsumerAvailable() {
        if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
            // abort read if no consumers are connected or if disconnect is initiated
            return false;
        }
        for(Consumer consumer : consumerList) {
            if (isConsumerAvailable(consumer)) {
                return true;
            }
        }
        return false;
    }

    private boolean isConsumerAvailable(Consumer consumer) {
        return consumer != null && !consumer.isBlocked() && consumer.getAvailablePermits() > 0;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
        consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, none) -> {
            messagesToReplay.add(ledgerId, entryId);
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}] Redelivering unacknowledged messages for consumer {}", consumer, messagesToReplay);
        }
        readMoreEntries();
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        positions.forEach(position -> messagesToReplay.add(position.getLedgerId(), position.getEntryId()));
        if (log.isDebugEnabled()) {
            log.debug("[{}] Redelivering unacknowledged messages for consumer {}", consumer, positions);
        }
        readMoreEntries();
    }

    @Override
    public void addUnAckedMessages(int numberOfMessages) {
        // don't block dispatching if maxUnackedMessages = 0
        if (maxUnackedMessages <= 0) {
            return;
        }
        int unAckedMessages = TOTAL_UNACKED_MESSAGES_UPDATER.addAndGet(this, numberOfMessages);
        if (unAckedMessages >= maxUnackedMessages
                && BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, FALSE, TRUE)) {
            // block dispatcher if it reaches maxUnAckMsg limit
            log.info("[{}] Dispatcher is blocked due to unackMessages {} reached to max {}", name,
                    TOTAL_UNACKED_MESSAGES_UPDATER.get(this), maxUnackedMessages);
        } else if (topic.getBrokerService().isBrokerDispatchingBlocked()
                && BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE) {
            // unblock dispatcher: if dispatcher is blocked due to broker-unackMsg limit and if it ack back enough
            // messages
            if (TOTAL_UNACKED_MESSAGES_UPDATER.get(this) < (topic.getBrokerService().maxUnackedMsgsPerDispatcher / 2)) {
                if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                    // it removes dispatcher from blocked list and unblocks dispatcher by scheduling read
                    topic.getBrokerService().unblockDispatchersOnUnAckMessages(Lists.newArrayList(this));
                }
            }
        } else if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE
                && unAckedMessages < maxUnackedMessages / 2) {
            // unblock dispatcher if it acks back enough messages
            if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                log.info("[{}] Dispatcher is unblocked", name);
                topic.getBrokerService().executor().submit(() -> readMoreEntries());
            }
        }
        // increment broker-level count
        topic.getBrokerService().addUnAckedMessages(this, numberOfMessages);
    }

    public boolean isBlockedDispatcherOnUnackedMsgs() {
        return BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE;
    }
    
    public void blockDispatcherOnUnackedMsgs() {
        BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.set(this, TRUE);
    }
    
    public void unBlockDispatcherOnUnackedMsgs() {
        BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.set(this, FALSE);
    }

    public int getTotalUnackedMessages() {
        return TOTAL_UNACKED_MESSAGES_UPDATER.get(this);
    }

    public String getName() {
        return name;
    }
    
    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherMultipleConsumers.class);
}
