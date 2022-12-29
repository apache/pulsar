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

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import com.google.common.collect.Lists;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.streamingdispatch.PendingReadEntryRequest;
import org.apache.pulsar.broker.service.streamingdispatch.StreamingDispatcher;
import org.apache.pulsar.broker.service.streamingdispatch.StreamingEntryReader;

/**
 * A {@link PersistentDispatcherMultipleConsumers} implemented {@link StreamingDispatcher}.
 * It'll use {@link StreamingEntryReader} to read new entries instead read as micro batch from managed ledger.
 */
@Slf4j
public class PersistentStreamingDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers
    implements StreamingDispatcher {

    private int sendingTaskCounter = 0;
    private final StreamingEntryReader streamingEntryReader = new StreamingEntryReader((ManagedCursorImpl) cursor,
            this, topic);
    private final Executor topicExecutor;

    public PersistentStreamingDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
                                                          Subscription subscription) {
        super(topic, cursor, subscription);
        this.topicExecutor = topic.getBrokerService().getTopicOrderedExecutor().chooseThread(topic.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void readEntryComplete(Entry entry, PendingReadEntryRequest ctx) {

        ReadType readType = (ReadType) ctx.ctx;
        if (ctx.isLast()) {
            readFailureBackoff.reduceToHalf();
            if (readType == ReadType.Normal) {
                havePendingRead = false;
            } else {
                havePendingReplayRead = false;
            }
        }

        if (readBatchSize < serviceConfig.getDispatcherMaxReadBatchSize()) {
            int newReadBatchSize = Math.min(readBatchSize * 2, serviceConfig.getDispatcherMaxReadBatchSize());
            if (log.isDebugEnabled()) {
                log.debug("[{}] Increasing read batch size from {} to {}", name, readBatchSize, newReadBatchSize);
            }
            readBatchSize = newReadBatchSize;
        }

        if (shouldRewindBeforeReadingOrReplaying && readType == ReadType.Normal) {
            // All consumers got disconnected before the completion of the read operation
            entry.release();
            cursor.rewind();
            shouldRewindBeforeReadingOrReplaying = false;
            readMoreEntries();
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Distributing a messages to {} consumers", name, consumerList.size());
        }

        cursor.seek(((ManagedLedgerImpl) cursor.getManagedLedger())
                .getNextValidPosition((PositionImpl) entry.getPosition()));

        long size = entry.getLength();
        updatePendingBytesToDispatch(size);
        // dispatch messages to a separate thread, but still in order for this subscription
        // sendMessagesToConsumers is responsible for running broker-side filters
        // that may be quite expensive
        if (serviceConfig.isDispatcherDispatchMessagesInSubscriptionThread()) {
            // setting sendInProgress here, because sendMessagesToConsumers will be executed
            // in a separate thread, and we want to prevent more reads
            acquireSendInProgress();
            dispatchMessagesThread.execute(safeRun(() -> {
                if (sendMessagesToConsumers(readType, Lists.newArrayList(entry), false)) {
                    readMoreEntries();
                } else {
                    updatePendingBytesToDispatch(-size);
                }
            }));
        } else {
            if (sendMessagesToConsumers(readType, Lists.newArrayList(entry), true)) {
                readMoreEntriesAsync();
            } else {
                updatePendingBytesToDispatch(-size);
            }
        }
        ctx.recycle();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void canReadMoreEntries(boolean withBackoff) {
        havePendingRead = false;
        topic.getBrokerService().executor().schedule(() -> {
            topicExecutor.execute(SafeRun.safeRun(() -> {
                synchronized (PersistentStreamingDispatcherMultipleConsumers.this) {
                    if (!havePendingRead) {
                        log.info("[{}] Scheduling read operation", name);
                        readMoreEntries();
                    } else {
                        log.info("[{}] Skipping read since we have pendingRead", name);
                    }
                }
            }));
        }, withBackoff
                ? readFailureBackoff.next() : 0, TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyConsumersEndOfTopic() {
        if (cursor.getNumberOfEntriesInBacklog(false) == 0) {
            // Topic has been terminated and there are no more entries to read
            // Notify the consumer only if all the messages were already acknowledged
            checkAndApplyReachedEndOfTopicOrTopicMigration(consumerList);
        }
    }

    @Override
    protected void cancelPendingRead() {
        if (havePendingRead && streamingEntryReader.cancelReadRequests()) {
            havePendingRead = false;
        }
    }

    @Override
    protected synchronized void acquireSendInProgress() {
        sendingTaskCounter++;
    }

    @Override
    protected synchronized void releaseSendInProgress() {
        sendingTaskCounter--;
    }

    @Override
    protected synchronized boolean isSendInProgress() {
        return sendingTaskCounter > 0;
    }

    @Override
    public synchronized void readMoreEntries() {
        if (isSendInProgress()) {
            // we cannot read more entries while sending the previous batch
            // otherwise we could re-read the same entries and send duplicates
            return;
        }
        // totalAvailablePermits may be updated by other threads
        int currentTotalAvailablePermits = totalAvailablePermits;
        if (currentTotalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            Pair<Integer, Long> calculateResult = calculateToRead(currentTotalAvailablePermits);
            int messagesToRead = calculateResult.getLeft();
            long bytesToRead = calculateResult.getRight();
            if (-1 == messagesToRead || bytesToRead == -1) {
                // Skip read as topic/dispatcher has exceed the dispatch rate or previous pending read hasn't complete.
                return;
            }

            Set<PositionImpl> messagesToReplayNow = getMessagesToReplayNow(messagesToRead);

            if (!messagesToReplayNow.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule replay of {} messages for {} consumers", name, messagesToReplayNow.size(),
                            consumerList.size());
                }

                havePendingReplayRead = true;
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
                    topic.getBrokerService().executor().execute(safeRun(this::readMoreEntries));
                }
            } else if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE) {
                log.debug("[{}] Dispatcher read is blocked due to unackMessages {} reached to max {}", name,
                        totalUnackedMessages, topic.getMaxUnackedMessagesOnSubscription());
            } else if (!havePendingRead) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule read of {} messages for {} consumers", name, messagesToRead,
                            consumerList.size());
                }
                havePendingRead = true;
                streamingEntryReader.asyncReadEntries(messagesToRead, bytesToRead,
                        ReadType.Normal);
            } else {
                log.debug("[{}] Cannot schedule next read until previous one is done", name);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer buffer is full, pause reading", name);
            }
        }
    }

}
