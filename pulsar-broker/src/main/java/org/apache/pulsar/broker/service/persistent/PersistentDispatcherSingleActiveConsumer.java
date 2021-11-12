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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.AbstractDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTrackerDisabled;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.Type;
import org.apache.pulsar.broker.transaction.exception.buffer.TransactionBufferException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentDispatcherSingleActiveConsumer extends AbstractDispatcherSingleActiveConsumer
        implements Dispatcher, ReadEntriesCallback {

    protected final PersistentTopic topic;
    protected final String name;
    private Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();

    protected volatile boolean havePendingRead = false;

    protected volatile int readBatchSize;
    protected final Backoff readFailureBackoff;
    private volatile ScheduledFuture<?> readOnActiveConsumerTask = null;

    private final RedeliveryTracker redeliveryTracker;

    public PersistentDispatcherSingleActiveConsumer(ManagedCursor cursor, SubType subscriptionType, int partitionIndex,
                                                    PersistentTopic topic, Subscription subscription) {
        super(subscriptionType, partitionIndex, topic.getName(), subscription,
                topic.getBrokerService().pulsar().getConfiguration(), cursor);
        this.topic = topic;
        this.name = topic.getName() + " / " + (cursor.getName() != null ? Codec.decode(cursor.getName())
                : ""/* NonDurableCursor doesn't have name */);
        this.readBatchSize = serviceConfig.getDispatcherMaxReadBatchSize();
        this.readFailureBackoff = new Backoff(serviceConfig.getDispatcherReadFailureBackoffInitialTimeInMs(),
            TimeUnit.MILLISECONDS, serviceConfig.getDispatcherReadFailureBackoffMaxTimeInMs(),
            TimeUnit.MILLISECONDS, serviceConfig.getDispatcherReadFailureBackoffMandatoryStopTimeInMs(),
            TimeUnit.MILLISECONDS);
        this.redeliveryTracker = RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
        this.initializeDispatchRateLimiterIfNeeded(Optional.empty());
    }

    protected void scheduleReadOnActiveConsumer() {
        cancelPendingRead();

        if (havePendingRead) {
            return;
        }

        // When a new consumer is chosen, start delivery from unacked message.
        // If there is any pending read operation, let it finish and then rewind

        if (subscriptionType != SubType.Failover || serviceConfig.getActiveConsumerFailoverDelayTimeMillis() <= 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Rewind cursor and read more entries without delay", name);
            }
            cursor.rewind();

            Consumer activeConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
            notifyActiveConsumerChanged(activeConsumer);
            readMoreEntries(activeConsumer);
            return;
        }

        // If subscription type is Failover, delay rewinding cursor and
        // reading more entries in order to prevent message duplication

        if (readOnActiveConsumerTask != null) {
            return;
        }

        readOnActiveConsumerTask = topic.getBrokerService().executor().schedule(() -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Rewind cursor and read more entries after {} ms delay", name,
                        serviceConfig.getActiveConsumerFailoverDelayTimeMillis());
            }
            cursor.rewind();

            Consumer activeConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
            notifyActiveConsumerChanged(activeConsumer);
            readMoreEntries(activeConsumer);
            readOnActiveConsumerTask = null;
        }, serviceConfig.getActiveConsumerFailoverDelayTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected boolean isConsumersExceededOnSubscription() {
        return isConsumersExceededOnSubscription(topic.getBrokerService(), topic.getName(), consumers.size());
    }

    @Override
    protected void cancelPendingRead() {
        if (havePendingRead && cursor.cancelPendingReadRequest()) {
            havePendingRead = false;
        }
    }

    @Override
    public void readEntriesComplete(final List<Entry> entries, Object obj) {
        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
            internalReadEntriesComplete(entries, obj);
        }));
    }

    public synchronized void internalReadEntriesComplete(final List<Entry> entries, Object obj) {
        Consumer readConsumer = (Consumer) obj;
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Got messages: {}", name, readConsumer, entries.size());
        }

        havePendingRead = false;
        isFirstRead = false;

        if (readBatchSize < serviceConfig.getDispatcherMaxReadBatchSize()) {
            int newReadBatchSize = Math.min(readBatchSize * 2, serviceConfig.getDispatcherMaxReadBatchSize());
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Increasing read batch size from {} to {}", name, readConsumer, readBatchSize,
                        newReadBatchSize);
            }

            readBatchSize = newReadBatchSize;
        }

        readFailureBackoff.reduceToHalf();

        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);

        if (isKeyHashRangeFiltered) {
            Iterator<Entry> iterator = entries.iterator();
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                byte[] key = peekStickyKey(entry.getDataBuffer());
                Consumer consumer = stickyKeyConsumerSelector.select(key);
                // Skip the entry if it's not for current active consumer.
                if (consumer == null || currentConsumer != consumer) {
                    entry.release();
                    iterator.remove();
                }
            }
        }

        if (currentConsumer == null || readConsumer != currentConsumer) {
            // Active consumer has changed since the read request has been issued. We need to rewind the cursor and
            // re-issue the read request for the new consumer
            if (log.isDebugEnabled()) {
                log.debug("[{}] rewind because no available consumer found", name);
            }
            entries.forEach(Entry::release);
            cursor.rewind();
            if (currentConsumer != null) {
                notifyActiveConsumerChanged(currentConsumer);
                readMoreEntries(currentConsumer);
            }
        } else {
            EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
            SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
            EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entries.size());
            filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, batchIndexesAcks, cursor, false);
            dispatchEntriesToConsumer(currentConsumer, entries, batchSizes, batchIndexesAcks, sendMessageInfo);
        }
    }

    protected void dispatchEntriesToConsumer(Consumer currentConsumer, List<Entry> entries,
                                             EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
                                             SendMessageInfo sendMessageInfo) {
        currentConsumer
            .sendMessages(entries, batchSizes, batchIndexesAcks, sendMessageInfo.getTotalMessages(),
                    sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(),
                    redeliveryTracker)
            .addListener(future -> {
                if (future.isSuccess()) {
                    int permits = dispatchThrottlingOnBatchMessageEnabled ? entries.size()
                            : sendMessageInfo.getTotalMessages();
                    // acquire message-dispatch permits for already delivered messages
                    if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
                        if (topic.getDispatchRateLimiter().isPresent()) {
                            topic.getDispatchRateLimiter().get().tryDispatchPermit(permits,
                                    sendMessageInfo.getTotalBytes());
                        }
                        dispatchRateLimiter.ifPresent(rateLimiter ->
                                rateLimiter.tryDispatchPermit(permits,
                                        sendMessageInfo.getTotalBytes()));
                    }

                    // Schedule a new read batch operation only after the previous batch has been written to the socket.
                    topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName,
                        SafeRun.safeRun(() -> {
                            synchronized (PersistentDispatcherSingleActiveConsumer.this) {
                                Consumer newConsumer = getActiveConsumer();
                                if (newConsumer != null && !havePendingRead) {
                                    readMoreEntries(newConsumer);
                                } else {
                                    log.debug(
                                            "[{}-{}] Ignoring write future complete."
                                                    + " consumerAvailable={} havePendingRead={}",
                                            name, newConsumer, newConsumer != null, havePendingRead);
                                }
                            }
                        }));
                }
            });
    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
            internalConsumerFlow(consumer);
        }));
    }

    private synchronized void internalConsumerFlow(Consumer consumer) {
        if (havePendingRead) {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Ignoring flow control message since we already have a pending read req", name,
                        consumer);
            }
        } else if (ACTIVE_CONSUMER_UPDATER.get(this) != consumer) {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Ignoring flow control message since consumer is not active partition consumer", name,
                        consumer);
            }
        } else if (readOnActiveConsumerTask != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Ignoring flow control message since consumer is waiting for cursor to be rewinded",
                        name, consumer);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Trigger new read after receiving flow control message", name, consumer);
            }
            readMoreEntries(consumer);
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer) {
        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
            internalRedeliverUnacknowledgedMessages(consumer);
        }));
    }

    private synchronized void internalRedeliverUnacknowledgedMessages(Consumer consumer) {
        if (consumer != ACTIVE_CONSUMER_UPDATER.get(this)) {
            log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: Only the active consumer can call resend",
                    name, consumer);
            return;
        }

        if (readOnActiveConsumerTask != null) {
            log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: consumer is waiting for cursor to be rewinded",
                    name, consumer);
            return;
        }

        cancelPendingRead();

        if (!havePendingRead) {
            cursor.rewind();
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Cursor rewinded, redelivering unacknowledged messages. ", name, consumer);
            }
            readMoreEntries(consumer);
        } else {
            log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: cancelPendingRequest on cursor failed", name,
                    consumer);
        }

    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        // We cannot redeliver single messages to single consumers to preserve ordering.
        positions.forEach(redeliveryTracker::addIfAbsent);
        redeliverUnacknowledgedMessages(consumer);
    }

    @Override
    protected void readMoreEntries(Consumer consumer) {
        // consumer can be null when all consumers are disconnected from broker.
        // so skip reading more entries if currently there is no active consumer.
        if (null == consumer) {
            return;
        }

        if (consumer.getAvailablePermits() > 0) {
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
                cursor.asyncReadEntriesOrWait(messagesToRead,
                        bytesToRead, this, consumer, topic.getMaxReadPosition());
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Consumer buffer is full, pause reading", name, consumer);
            }
        }
    }

    protected Pair<Integer, Long> calculateToRead(Consumer consumer) {
        int availablePermits = consumer.getAvailablePermits();
        if (!consumer.isWritable()) {
            // If the connection is not currently writable, we issue the read request anyway, but for a single
            // message. The intent here is to keep use the request as a notification mechanism while avoiding to
            // read and dispatch a big batch of messages which will need to wait before getting written to the
            // socket.
            availablePermits = 1;
        }

        int messagesToRead = Math.min(availablePermits, readBatchSize);
        long bytesToRead = serviceConfig.getDispatcherMaxReadSizeBytes();
        // if turn of precise dispatcher flow control, adjust the records to read
        if (consumer.isPreciseDispatcherFlowControl()) {
            int avgMessagesPerEntry = consumer.getAvgMessagesPerEntry();
            messagesToRead = Math.min((int) Math.ceil(availablePermits * 1.0 / avgMessagesPerEntry), readBatchSize);
        }

        // throttle only if: (1) cursor is not active (or flag for throttle-nonBacklogConsumer is enabled) bcz
        // active-cursor reads message from cache rather from bookkeeper (2) if topic has reached message-rate
        // threshold: then schedule the read after MESSAGE_RATE_BACKOFF_MS
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            if (topic.getDispatchRateLimiter().isPresent()
                    && topic.getDispatchRateLimiter().get().isDispatchRateLimitingEnabled()) {
                DispatchRateLimiter topicRateLimiter = topic.getDispatchRateLimiter().get();
                if (!topicRateLimiter.hasMessageDispatchPermit()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded topic message-rate {}/{}, schedule after a {}", name,
                                topicRateLimiter.getDispatchRateOnMsg(), topicRateLimiter.getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    topic.getBrokerService().executor().schedule(() -> {
                        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
                        if (currentConsumer != null && !havePendingRead) {
                            readMoreEntries(currentConsumer);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Skipping read retry for topic: Current Consumer {},"
                                                + " havePendingRead {}",
                                        topic.getName(), currentConsumer, havePendingRead);
                            }
                        }
                    }, MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
                    return Pair.of(-1, -1L);
                } else {

                    Pair<Integer, Long> calculateResult = computeReadLimits(messagesToRead,
                            (int) topicRateLimiter.getAvailableDispatchRateLimitOnMsg(),
                            bytesToRead, topicRateLimiter.getAvailableDispatchRateLimitOnByte());

                    messagesToRead = calculateResult.getLeft();
                    bytesToRead = calculateResult.getRight();

                }
            }

            if (dispatchRateLimiter.isPresent() && dispatchRateLimiter.get().isDispatchRateLimitingEnabled()) {
                if (!dispatchRateLimiter.get().hasMessageDispatchPermit()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded subscription message-rate {}/{},"
                                        + " schedule after a {}",
                                name, dispatchRateLimiter.get().getDispatchRateOnMsg(),
                                dispatchRateLimiter.get().getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    topic.getBrokerService().executor().schedule(() -> {
                        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
                        if (currentConsumer != null && !havePendingRead) {
                            readMoreEntries(currentConsumer);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Skipping read retry: Current Consumer {}, havePendingRead {}",
                                        topic.getName(), currentConsumer, havePendingRead);
                            }
                        }
                    }, MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
                    return Pair.of(-1, -1L);
                } else {

                    Pair<Integer, Long> calculateResult = computeReadLimits(messagesToRead,
                            (int) dispatchRateLimiter.get().getAvailableDispatchRateLimitOnMsg(),
                            bytesToRead, dispatchRateLimiter.get().getAvailableDispatchRateLimitOnByte());

                    messagesToRead = calculateResult.getLeft();
                    bytesToRead = calculateResult.getRight();
                }
            }
        }

        // If messagesToRead is 0 or less, correct it to 1 to prevent IllegalArgumentException
        return Pair.of(Math.max(messagesToRead, 1), Math.max(bytesToRead, 1));
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
            internalReadEntriesFailed(exception, ctx);
        }));
    }

    private synchronized void internalReadEntriesFailed(ManagedLedgerException exception, Object ctx) {
        havePendingRead = false;
        Consumer c = (Consumer) ctx;

        long waitTimeMillis = readFailureBackoff.next();

        if (exception instanceof NoMoreEntriesToReadException) {
            if (cursor.getNumberOfEntriesInBacklog(false) == 0) {
                // Topic has been terminated and there are no more entries to read
                // Notify the consumer only if all the messages were already acknowledged
                consumers.forEach(Consumer::reachedEndOfTopic);
            }
        } else if (exception.getCause() instanceof TransactionBufferException.TransactionNotSealedException) {
            waitTimeMillis = 1;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error reading transaction entries : {}, - Retrying to read in {} seconds", name,
                        exception.getMessage(), waitTimeMillis / 1000.0);
            }
        } else if (!(exception instanceof TooManyRequestsException)) {
            log.error("[{}-{}] Error reading entries at {} : {} - Retrying to read in {} seconds", name, c,
                    cursor.getReadPosition(), exception.getMessage(), waitTimeMillis / 1000.0);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Got throttled by bookies while reading at {} : {} - Retrying to read in {} seconds",
                        name, c, cursor.getReadPosition(), exception.getMessage(), waitTimeMillis / 1000.0);
            }
        }

        checkNotNull(c);

        // Reduce read batch size to avoid flooding bookies with retries
        readBatchSize = serviceConfig.getDispatcherMinReadBatchSize();

        topic.getBrokerService().executor().schedule(() -> {

            // Jump again into dispatcher dedicated thread
            topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
                synchronized (PersistentDispatcherSingleActiveConsumer.this) {
                    Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
                    // we should retry the read if we have an active consumer and there is no pending read
                    if (currentConsumer != null && !havePendingRead) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}-{}] Retrying read operation", name, c);
                        }
                        if (currentConsumer != c) {
                            notifyActiveConsumerChanged(currentConsumer);
                        }
                        readMoreEntries(currentConsumer);
                    } else {
                        log.info("[{}-{}] Skipping read retry: Current Consumer {}, havePendingRead {}", name, c,
                                currentConsumer, havePendingRead);
                    }
                }
            }));
        }, waitTimeMillis, TimeUnit.MILLISECONDS);

    }

    @Override
    public void addUnAckedMessages(int unAckMessages) {
        // No-op
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
    public void updateRateLimiter(DispatchRate dispatchRate) {
        if (!this.dispatchRateLimiter.isPresent() && dispatchRate != null) {
            this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(topic, Type.SUBSCRIPTION));
        }
        this.dispatchRateLimiter.ifPresent(limiter -> {
            if (dispatchRate != null) {
                this.dispatchRateLimiter.get().updateDispatchRate(dispatchRate);
            } else {
                this.dispatchRateLimiter.get().updateDispatchRate();
            }
        });
    }

    @Override
    public void initializeDispatchRateLimiterIfNeeded(Optional<Policies> policies) {
        if (!dispatchRateLimiter.isPresent() && DispatchRateLimiter
                .isDispatchRateNeeded(topic.getBrokerService(), policies, topic.getName(), Type.SUBSCRIPTION)) {
            this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(topic, Type.SUBSCRIPTION));
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        IS_CLOSED_UPDATER.set(this, TRUE);
        dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);
        return disconnectAllConsumers();
    }

    @Override
    public boolean checkAndUnblockIfStuck() {
        Consumer consumer = ACTIVE_CONSUMER_UPDATER.get(this);
        if (consumer == null || cursor.checkAndUpdateReadPositionChanged()) {
            return false;
        }
        int totalAvailablePermits = consumer.getAvailablePermits();
        // consider dispatch is stuck if : dispatcher has backlog, available-permits and there is no pending read
        if (totalAvailablePermits > 0 && !havePendingRead && cursor.getNumberOfEntriesInBacklog(false) > 0) {
            log.warn("{}-{} Dispatcher is stuck and unblocking by issuing reads", topic.getName(), name);
            readMoreEntries(consumer);
            return true;
        }
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherSingleActiveConsumer.class);
}
