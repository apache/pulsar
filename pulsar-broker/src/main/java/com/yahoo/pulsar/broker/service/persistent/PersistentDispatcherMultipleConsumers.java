/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service.persistent;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.common.util.Codec;
import com.yahoo.pulsar.broker.service.Consumer;
import com.yahoo.pulsar.broker.service.Dispatcher;
import com.yahoo.pulsar.broker.service.BrokerServiceException;
import com.yahoo.pulsar.client.impl.Backoff;
import com.yahoo.pulsar.utils.CopyOnWriteArrayList;

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
    private TreeSet<PositionImpl> messagesToReplay;

    private int currentConsumerRoundRobinIndex = 0;
    private boolean havePendingRead = false;
    private boolean havePendingReplayRead = false;
    private boolean shouldRewindBeforeReadingOrReplaying = false;
    private final String name;

    private int totalAvailablePermits = 0;
    private int readBatchSize;
    private final Backoff readFailureBackoff = new Backoff(15, TimeUnit.SECONDS, 1, TimeUnit.MINUTES);

    enum ReadType {
        Normal, Replay
    }

    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor) {
        this.cursor = cursor;
        this.name = topic.getName() + " / " + Codec.decode(cursor.getName());
        this.topic = topic;
        this.messagesToReplay = Sets.newTreeSet();
        this.readBatchSize = MaxReadBatchSize;
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) {
        if (closeFuture != null) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer ", name, consumer);
            consumer.disconnect();
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
                consumer.getPendingAcks().forEach((pendingMessages, totalMsg) -> {
                    messagesToReplay.add(pendingMessages);
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

    private void readMoreEntries() {
        if (totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            int messagesToRead = Math.min(totalAvailablePermits, readBatchSize);

            if (!messagesToReplay.isEmpty()) {
                if (havePendingReplayRead) {
                    log.debug("[{}] Skipping replay while awaiting previous read to complete", name);
                    return;
                }

                Set<PositionImpl> messagesToReplayNow = ImmutableSet
                        .copyOf(Iterables.limit(messagesToReplay, messagesToRead));

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule replay of {} messages for {} consumers", name, messagesToReplayNow.size(),
                            consumerList.size());
                }

                havePendingReplayRead = true;
                Set<? extends Position> deletedMessages = cursor.asyncReplayEntries(messagesToReplayNow, this,
                        ReadType.Replay);
                // clear already acked positions from replay bucket
                messagesToReplay.removeAll(deletedMessages);
                // if all the entries are acked-entries and cleared up from messagesToReplay, try to read
                // next entries as readCompletedEntries-callback was never called 
                if ((messagesToReplayNow.size() - deletedMessages.size()) == 0) {
                    havePendingReplayRead = false;
                    readMoreEntries();
                }
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
    public synchronized CompletableFuture<Void> disconnect() {
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
    public synchronized void connect() {
        closeFuture = null;
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
                int msgSent = c.sendMessages(entries.subList(start, start + messagesForC)).getRight();

                if (readType == ReadType.Replay) {
                    entries.subList(start, start + messagesForC).forEach(entry -> {
                        messagesToReplay.remove((PositionImpl) entry.getPosition());
                    });
                }
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
                messagesToReplay.add((PositionImpl) entry.getPosition());
                entry.release();
            });
        }

        readMoreEntries();
    }

    @Override
    public synchronized void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

        ReadType readType = (ReadType) ctx;
        long waitTimeMillis = readFailureBackoff.next();

        if (!(exception instanceof TooManyRequestsException)) {
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
                messagesToReplay.removeIf(current -> current.compareTo(markDeletePosition) <= 0);
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
     * 1. sorted-list: consumers stored in sorted-list: max-priority stored first 
     * 2. currentConsumerRoundRobinIndex: it always stores last served consumer-index
     * 3. resultingAvailableConsumerIndex: traversal index. we always start searching availableConsumer from the  
     *    beginning of sorted-list and update resultingAvailableConsumerIndex according searching-traversal
     *    
     * Each time getNextConsumer() is called:<p>
     * 1. It always starts to traverse from the max-priority consumer (first element) from sorted-list
     * 2. Consumers on same priority-level will be treated equally and it tries to pick one of them in round-robin manner
     * 3. If consumer is not available on given priority-level then it will go to the next lower priority-level consumers
     * 4. Optimization: <p>
     *    A. Consumers on same priority-level must be treated equally => dispatch message round-robin to them: 
     *       [if Consumer of resultingAvailableConsumerIndex(current-traversal-index) has the same 
     *       priority-level as consumer of currentConsumerRoundRobinIndex(last-Served-Consumer-Index)] 
     *       <b>Dispatching in Round-Robin:</b> then it means we should do round-robin and skip all the consumers before 
     *          currentConsumerRoundRobinIndex (as they are already served previously)
     *               a. if found: if we found availableConsumer on the same priority-level after currentConsumerRoundRobinIndex 
     *                            then return that consumer and update currentConsumerRoundRobinIndex with that consumer-index
     *               b. else not_found: if we don't find any consumer on that same-priority level after currentConsumerRoundRobinIndex
     *                      - a. check skipped consumers: check skipped consumer (4.A.a) which are on index before than currentConsumerRoundRobinIndex
     *                      - b. next priority-level: if not found in previous step: then it means no consumer available in prior level. So, move to 
     *                           next lower priority level and try to find next-available consumer as per 4.A
     * 
     * </pre>
     * 
     * @return nextAvailableConsumer
     */
    public Consumer getNextConsumer() {
        if (consumerList.isEmpty() || closeFuture != null) {
            // abort read if no consumers are connected or if disconnect is initiated
            return null;
        }

        if (currentConsumerRoundRobinIndex >= consumerList.size()) {
            currentConsumerRoundRobinIndex = 0;
        }

        // index of resulting consumer which will be returned
        int resultingAvailableConsumerIndex = 0;
        boolean scanFromBeginningIfCurrentConsumerNotAvailable = true;
        int firstConsumerIndexOfCurrentPriorityLevel = -1;
        do {
            int priorityLevel = consumerList.get(currentConsumerRoundRobinIndex).getPriorityLevel()
                    - consumerList.get(resultingAvailableConsumerIndex).getPriorityLevel();

            boolean isSamePriorityLevel = priorityLevel == 0;
            // store first-consumer index with same-priority as currentConsumerRoundRobinIndex
            if (isSamePriorityLevel && firstConsumerIndexOfCurrentPriorityLevel == -1) {
                firstConsumerIndexOfCurrentPriorityLevel = resultingAvailableConsumerIndex;
            }

            // skip already served same-priority-consumer to select consumer in round-robin manner
            resultingAvailableConsumerIndex = (isSamePriorityLevel
                    && currentConsumerRoundRobinIndex > resultingAvailableConsumerIndex)
                            ? currentConsumerRoundRobinIndex : resultingAvailableConsumerIndex;

            // if resultingAvailableConsumerIndex moved ahead of currentConsumerRoundRobinIndex: then we should
            // check skipped consumer which had same priority as currentConsumerRoundRobinIndex consumer
            boolean isLastConsumerBlocked = (currentConsumerRoundRobinIndex == (consumerList.size() - 1)
                    && !isConsumerAvailable(consumerList.get(currentConsumerRoundRobinIndex)));
            boolean shouldScanCurrentLevel = priorityLevel < 0
                    /* means moved to next lower-priority-level */ || isLastConsumerBlocked;
            if (shouldScanCurrentLevel && scanFromBeginningIfCurrentConsumerNotAvailable) {
                for (int i = firstConsumerIndexOfCurrentPriorityLevel; i < currentConsumerRoundRobinIndex; i++) {
                    Consumer nextConsumer = consumerList.get(i);
                    if (isConsumerAvailable(nextConsumer)) {
                        currentConsumerRoundRobinIndex = i + 1;
                        return nextConsumer;
                    }
                }
                // now, we have scanned from the beginning: flip the flag to avoid scan again
                scanFromBeginningIfCurrentConsumerNotAvailable = false;
            }

            Consumer nextConsumer = consumerList.get(resultingAvailableConsumerIndex);
            if (isConsumerAvailable(nextConsumer)) {
                currentConsumerRoundRobinIndex = resultingAvailableConsumerIndex + 1;
                return nextConsumer;
            }
            if (++resultingAvailableConsumerIndex >= consumerList.size()) {
                break;
            }
        } while (true);

        // not found unblocked consumer
        return null;
    }
    
    /**
     * returns true only if {@link consumerList} has atleast one unblocked consumer and have available permits
     * 
     * @return
     */
    private boolean isAtleastOneConsumerAvailable() {
        if (consumerList.isEmpty() || closeFuture != null) {
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
        consumer.getPendingAcks().forEach((pendingMessages, totalMsg) -> {
            messagesToReplay.add(pendingMessages);
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}] Redelivering unacknowledged messages for consumer ", consumer);
        }
        readMoreEntries();
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        messagesToReplay.addAll(positions);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Redelivering unacknowledged messages for consumer ", consumer);
        }
        readMoreEntries();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherMultipleConsumers.class);
}
