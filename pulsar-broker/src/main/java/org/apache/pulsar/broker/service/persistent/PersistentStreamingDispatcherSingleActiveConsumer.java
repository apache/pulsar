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

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import com.google.common.collect.Lists;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.streamingdispatch.PendingReadEntryRequest;
import org.apache.pulsar.broker.service.streamingdispatch.StreamingDispatcher;
import org.apache.pulsar.broker.service.streamingdispatch.StreamingEntryReader;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;

/**
 * A {@link PersistentDispatcherSingleActiveConsumer} implemented {@link StreamingDispatcher}.
 * It'll use {@link StreamingEntryReader} to read new entries instead read as micro batch from managed ledger.
 */
@Slf4j
public class PersistentStreamingDispatcherSingleActiveConsumer extends PersistentDispatcherSingleActiveConsumer
        implements StreamingDispatcher {

    private final StreamingEntryReader streamingEntryReader = new StreamingEntryReader((ManagedCursorImpl) cursor,
            this, topic);

    public PersistentStreamingDispatcherSingleActiveConsumer(ManagedCursor cursor, SubType subscriptionType,
                                                             int partitionIndex, PersistentTopic topic,
                                                             Subscription subscription) {
        super(cursor, subscriptionType, partitionIndex, topic, subscription);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void canReadMoreEntries(boolean withBackoff) {
        havePendingRead = false;
        topic.getBrokerService().executor().schedule(() -> {
            topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
                synchronized (PersistentStreamingDispatcherSingleActiveConsumer.this) {
                    Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
                    if (currentConsumer != null && !havePendingRead) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}-{}] Scheduling read ", name, currentConsumer);
                        }
                        readMoreEntries(currentConsumer);
                    } else {
                        log.info("[{}-{}] Skipping read as we still havePendingRead {}", name,
                                currentConsumer, havePendingRead);
                    }
                }
            }));
        }, withBackoff
                ? readFailureBackoff.next() : 0, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void cancelPendingRead() {
        if (havePendingRead && streamingEntryReader.cancelReadRequests()) {
            havePendingRead = false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void notifyConsumersEndOfTopic() {
        if (cursor.getNumberOfEntriesInBacklog(false) == 0) {
            // Topic has been terminated and there are no more entries to read
            // Notify the consumer only if all the messages were already acknowledged
            consumers.forEach(Consumer::reachedEndOfTopic);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readEntryComplete(Entry entry, PendingReadEntryRequest ctx) {
        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(name, safeRun(() -> {
            internalReadEntryComplete(entry, ctx);
        }));
    }

    public synchronized void internalReadEntryComplete(Entry entry, PendingReadEntryRequest ctx) {
        if (ctx.isLast()) {
            readFailureBackoff.reduceToHalf();
            havePendingRead = false;
        }

        isFirstRead = false;

        if (readBatchSize < serviceConfig.getDispatcherMaxReadBatchSize()) {
            int newReadBatchSize = Math.min(readBatchSize * 2, serviceConfig.getDispatcherMaxReadBatchSize());
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Increasing read batch size from {} to {}", name,
                        ((Consumer) ctx.ctx).consumerName(), readBatchSize, newReadBatchSize);
            }
            readBatchSize = newReadBatchSize;
        }

        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);

        if (isKeyHashRangeFiltered) {
            byte[] key = peekStickyKey(entry.getDataBuffer());
            Consumer consumer = stickyKeyConsumerSelector.select(key);
            // Skip the entry if it's not for current active consumer.
            if (consumer == null || currentConsumer != consumer) {
                entry.release();
                return;
            }
        }
        Consumer consumer = (Consumer) ctx.ctx;
        ctx.recycle();
        if (currentConsumer == null || consumer != currentConsumer) {
            // Active consumer has changed since the read request has been issued. We need to rewind the cursor and
            // re-issue the read request for the new consumer
            if (log.isDebugEnabled()) {
                log.debug("[{}] Rewind because no available consumer found to dispatch message to.", name);
            }

            entry.release();
            streamingEntryReader.cancelReadRequests();
            havePendingRead = false;
            if (currentConsumer != null) {
                notifyActiveConsumerChanged(currentConsumer);
                readMoreEntries(currentConsumer);
            }
        } else {
            EntryBatchSizes batchSizes = EntryBatchSizes.get(1);
            SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
            EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(1);
            filterEntriesForConsumer(Lists.newArrayList(entry), batchSizes, sendMessageInfo, batchIndexesAcks,
                    cursor, false, consumer);
            // Update cursor's read position.
            cursor.seek(((ManagedLedgerImpl) cursor.getManagedLedger())
                    .getNextValidPosition((PositionImpl) entry.getPosition()));
            dispatchEntriesToConsumer(currentConsumer, Lists.newArrayList(entry), batchSizes,
                    batchIndexesAcks, sendMessageInfo, DEFAULT_CONSUMER_EPOCH);
        }
    }

    @Override
    protected void readMoreEntries(Consumer consumer) {
        // consumer can be null when all consumers are disconnected from broker.
        // so skip reading more entries if currently there is no active consumer.
        if (null == consumer) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Skipping read for the topic, Due to the current consumer is null", topic.getName());
            }
            return;
        }
        if (havePendingRead) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Skipping read for the topic, Due to we have pending read.", topic.getName());
            }
            return;
        }

        if (consumer.getAvailablePermits() > 0) {
            synchronized (this) {
                if (havePendingRead) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Skipping read for the topic, Due to we have pending read.", topic.getName());
                    }
                    return;
                }

                Pair<Integer, Long> calculateResult = calculateToRead(consumer);
                int messagesToRead = calculateResult.getLeft();
                long bytesToRead = calculateResult.getRight();


                if (-1 == messagesToRead || bytesToRead == -1) {
                    // Skip read as topic/dispatcher has exceed the dispatch rate.
                    return;
                }

                // Schedule read
                if (log.isDebugEnabled()) {
                    log.debug("[{}-{}] Schedule read of {} messages", name, consumer, messagesToRead);
                }
                havePendingRead = true;

                if (consumer.readCompacted()) {
                    topic.getCompactedTopic().asyncReadEntriesOrWait(cursor, messagesToRead, isFirstRead,
                            this, consumer);
                } else {
                    streamingEntryReader.asyncReadEntries(messagesToRead, bytesToRead, consumer);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Consumer buffer is full, pause reading", name, consumer);
            }
        }
    }

}
