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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.yahoo.pulsar.common.api.proto.PulsarApi;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
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

    private int consumerIndex = 0;
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
        if (totalAvailablePermits > 0 && isUnblockedConsumerAvailable()) {
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

        while (entriesToDispatch > 0 && totalAvailablePermits > 0 && isUnblockedConsumerAvailable()) {
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
                c.sendMessages(entries.subList(start, start + messagesForC));

                if (readType == ReadType.Replay) {
                    entries.subList(start, start + messagesForC).forEach(entry -> {
                        messagesToReplay.remove((PositionImpl) entry.getPosition());
                    });
                }
                start += messagesForC;
                entriesToDispatch -= messagesForC;
                totalAvailablePermits -= messagesForC;
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

    private Consumer getNextConsumer() {
        if (consumerList.isEmpty() || closeFuture != null) {
            // abort read if no consumers are connected or if disconnect is initiated
            return null;
        }

        if (consumerIndex >= consumerList.size()) {
            consumerIndex = 0;
        }
        
        // find next available unblocked consumer
        int unblockedConsumerIndex = consumerIndex;
        do {
            if (!consumerList.get(unblockedConsumerIndex).isBlocked()) {
                consumerIndex = unblockedConsumerIndex;
                return consumerList.get(consumerIndex++);
            }
            if (++unblockedConsumerIndex >= consumerList.size()) {
                unblockedConsumerIndex = 0;
            }
        } while (unblockedConsumerIndex != consumerIndex);

        // not found unblocked consumer
        return null;
    }
    
    /**
     * returns true only if {@link consumerList} has atleast one unblocked consumer 
     * 
     * @return
     */
    private boolean isUnblockedConsumerAvailable() {
        if (consumerList.isEmpty() || closeFuture != null) {
            // abort read if no consumers are connected or if disconnect is initiated
            return false;
        }
        Iterator<Consumer> consumerIterator = consumerList.iterator();
        while (consumerIterator.hasNext()) {
            if (!consumerIterator.next().isBlocked()) {
                return true;
            }
        }
        return false;
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
