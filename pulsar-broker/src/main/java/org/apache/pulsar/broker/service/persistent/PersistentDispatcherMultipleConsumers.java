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

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.Range;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTracker;
import org.apache.pulsar.broker.service.AbstractDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.InMemoryRedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTrackerDisabled;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.Type;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.collections.ConcurrentSortedLongPairSet;
import org.apache.pulsar.common.util.collections.LongPairSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PersistentDispatcherMultipleConsumers extends AbstractDispatcherMultipleConsumers implements Dispatcher, ReadEntriesCallback {

    protected final PersistentTopic topic;
    protected final ManagedCursor cursor;
    protected volatile Range<PositionImpl> lastIndividualDeletedRangeFromCursorRecovery;

    private CompletableFuture<Void> closeFuture = null;
    LongPairSet messagesToRedeliver = new ConcurrentSortedLongPairSet(128, 2);
    protected final RedeliveryTracker redeliveryTracker;

    private Optional<DelayedDeliveryTracker> delayedDeliveryTracker = Optional.empty();

    private volatile boolean havePendingRead = false;
    private volatile boolean havePendingReplayRead = false;
    private boolean shouldRewindBeforeReadingOrReplaying = false;
    protected final String name;

    protected static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> TOTAL_AVAILABLE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "totalAvailablePermits");
    protected volatile int totalAvailablePermits = 0;
    private volatile int readBatchSize;
    private final Backoff readFailureBackoff = new Backoff(15, TimeUnit.SECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> TOTAL_UNACKED_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "totalUnackedMessages");
    private volatile int totalUnackedMessages = 0;
    private volatile int blockedDispatcherOnUnackedMsgs = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "blockedDispatcherOnUnackedMsgs");
    protected final ServiceConfiguration serviceConfig;
    protected Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();

    protected TransactionReader transactionReader;

    enum ReadType {
        Normal, Replay
    }

    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor, Subscription subscription) {
        super(subscription);
        this.serviceConfig = topic.getBrokerService().pulsar().getConfiguration();
        this.cursor = cursor;
        this.lastIndividualDeletedRangeFromCursorRecovery = cursor.getLastIndividualDeletedRange();
        this.name = topic.getName() + " / " + Codec.decode(cursor.getName());
        this.topic = topic;
        this.redeliveryTracker = this.serviceConfig.isSubscriptionRedeliveryTrackerEnabled()
                ? new InMemoryRedeliveryTracker()
                : RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
        this.readBatchSize = serviceConfig.getDispatcherMaxReadBatchSize();
        this.initializeDispatchRateLimiterIfNeeded(Optional.empty());
        this.transactionReader = new TransactionReader(topic, cursor);
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
            messagesToRedeliver.clear();
        }

        if (isConsumersExceededOnSubscription()) {
            log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit", name);
            throw new ConsumerBusyException("Subscription reached max consumers limit");
        }

        consumerList.add(consumer);
        consumerList.sort((c1, c2) -> c1.getPriorityLevel() - c2.getPriorityLevel());
        consumerSet.add(consumer);
    }

    private boolean isConsumersExceededOnSubscription() {
        Policies policies = null;
        Integer maxConsumersPerSubscription = null;
        try {
            maxConsumersPerSubscription = Optional.ofNullable(topic.getBrokerService()
                    .getTopicPolicies(TopicName.get(topic.getName())))
                    .map(TopicPolicies::getMaxConsumersPerSubscription)
                    .orElse(null);

            if (maxConsumersPerSubscription == null) {
                // Use getDataIfPresent from zk cache to make the call non-blocking and prevent deadlocks in addConsumer
                policies = topic.getBrokerService().pulsar().getConfigurationCache().policiesCache()
                        .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(topic.getName()).getNamespace()));
                if (policies == null) {
                    policies = new Policies();
                }
            }
        } catch (Exception e) {
            policies = new Policies();
        }

        if (maxConsumersPerSubscription == null) {
            maxConsumersPerSubscription = policies.max_consumers_per_subscription > 0 ?
                    policies.max_consumers_per_subscription :
                    serviceConfig.getMaxConsumersPerSubscription();
        }

        if (maxConsumersPerSubscription > 0 && maxConsumersPerSubscription <= consumerList.size()) {
            return true;
        }
        return false;
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

                messagesToRedeliver.clear();
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
                consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, none) -> {
                    messagesToRedeliver.add(ledgerId, entryId);
                    redeliveryTracker.addIfAbsent(PositionImpl.get(ledgerId, entryId));
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
            log.debug("[{}-{}] Trigger new read after receiving flow control message with permits {}", name, consumer,
                    totalAvailablePermits);
        }
        readMoreEntries();
    }

    public void readMoreEntries() {
        // totalAvailablePermits may be updated by other threads
        int currentTotalAvailablePermits = totalAvailablePermits;
        if (currentTotalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            int messagesToRead = Math.min(currentTotalAvailablePermits, readBatchSize);

            Consumer c = getRandomConsumer();
            // if turn on precise dispatcher flow control, adjust the record to read
            if (c != null && c.isPreciseDispatcherFlowControl()) {
                messagesToRead = Math.min((int) Math.ceil(currentTotalAvailablePermits * 1.0 / c.getAvgMessagesPerEntry()), readBatchSize);
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
                if (topic.getDispatchRateLimiter().isPresent() && topic.getDispatchRateLimiter().get().isDispatchRateLimitingEnabled()) {
                    DispatchRateLimiter topicRateLimiter = topic.getDispatchRateLimiter().get();
                    if (!topicRateLimiter.hasMessageDispatchPermit()) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] message-read exceeded topic message-rate {}/{}, schedule after a {}", name,
                                    topicRateLimiter.getDispatchRateOnMsg(), topicRateLimiter.getDispatchRateOnByte(),
                                    MESSAGE_RATE_BACKOFF_MS);
                        }
                        topic.getBrokerService().executor().schedule(() -> readMoreEntries(), MESSAGE_RATE_BACKOFF_MS,
                                TimeUnit.MILLISECONDS);
                        return;
                    } else {
                        // if dispatch-rate is in msg then read only msg according to available permit
                        long availablePermitsOnMsg = topicRateLimiter.getAvailableDispatchRateLimitOnMsg();
                        if (availablePermitsOnMsg > 0) {
                            messagesToRead = Math.min(messagesToRead, (int) availablePermitsOnMsg);
                        }
                    }
                }

                if (dispatchRateLimiter.isPresent() && dispatchRateLimiter.get().isDispatchRateLimitingEnabled()) {
                    if (!dispatchRateLimiter.get().hasMessageDispatchPermit()) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] message-read exceeded subscription message-rate {}/{}, schedule after a {}", name,
                                dispatchRateLimiter.get().getDispatchRateOnMsg(), dispatchRateLimiter.get().getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                        }
                        topic.getBrokerService().executor().schedule(() -> readMoreEntries(), MESSAGE_RATE_BACKOFF_MS,
                            TimeUnit.MILLISECONDS);
                        return;
                    } else {
                        // if dispatch-rate is in msg then read only msg according to available permit
                        long availablePermitsOnMsg = dispatchRateLimiter.get().getAvailableDispatchRateLimitOnMsg();
                        if (availablePermitsOnMsg > 0) {
                            messagesToRead = Math.min(messagesToRead, (int) availablePermitsOnMsg);
                        }
                    }
                }

            }

            if (havePendingReplayRead) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Skipping replay while awaiting previous read to complete", name);
                }
                return;
            }

            // If messagesToRead is 0 or less, correct it to 1 to prevent IllegalArgumentException
            messagesToRead = Math.max(messagesToRead, 1);
            Set<PositionImpl> messagesToReplayNow = getMessagesToReplayNow(messagesToRead);

            if (!messagesToReplayNow.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule replay of {} messages for {} consumers", name, messagesToReplayNow.size(),
                            consumerList.size());
                }

                havePendingReplayRead = true;
                Set<? extends Position> deletedMessages = topic.isDelayedDeliveryEnabled() ?
                        asyncReplayEntriesInOrder(messagesToReplayNow) : asyncReplayEntries(messagesToReplayNow);
                // clear already acked positions from replay bucket

                deletedMessages.forEach(position -> messagesToRedeliver.remove(((PositionImpl) position).getLedgerId(),
                        ((PositionImpl) position).getEntryId()));
                // if all the entries are acked-entries and cleared up from messagesToRedeliver, try to read
                // next entries as readCompletedEntries-callback was never called
                if ((messagesToReplayNow.size() - deletedMessages.size()) == 0) {
                    havePendingReplayRead = false;
                    readMoreEntries();
                }
            } else if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE) {
                log.warn("[{}] Dispatcher read is blocked due to unackMessages {} reached to max {}", name,
                        totalUnackedMessages, topic.getMaxUnackedMessagesOnSubscription());
            } else if (transactionReader.havePendingTxnToRead()) {
                transactionReader.read(messagesToRead, ReadType.Normal, this);
            } else if (!havePendingRead) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule read of {} messages for {} consumers", name, messagesToRead,
                            consumerList.size());
                }
                havePendingRead = true;
                cursor.asyncReadEntriesOrWait(messagesToRead, serviceConfig.getDispatcherMaxReadSizeBytes(), this,
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
            if (havePendingRead && cursor.cancelPendingReadRequest()) {
                havePendingRead = false;
            }
        }
        return closeFuture;
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
    public synchronized void readEntriesComplete(List<Entry> entries, Object ctx) {
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

    protected void sendMessagesToConsumers(ReadType readType, List<Entry> entries) {

        if (needTrimAckedMessages()) {
            cursor.trimDeletedEntries(entries);
        }

        int entriesToDispatch = entries.size();
        // Trigger read more messages
        if (entriesToDispatch == 0) {
            readMoreEntries();
            return;
        }
        int start = 0;
        long totalMessagesSent = 0;
        long totalBytesSent = 0;

        while (entriesToDispatch > 0 && totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            Consumer c = getNextConsumer();
            if (c == null) {
                // Do nothing, cursor will be rewind at reconnection
                log.info("[{}] rewind because no available consumer found from total {}", name, consumerList.size());
                entries.subList(start, entries.size()).forEach(Entry::release);
                cursor.rewind();
                return;
            }

            // round-robin dispatch batch size for this consumer
            int availablePermits = c.isWritable() ? c.getAvailablePermits() : 1;
            if (log.isDebugEnabled() && !c.isWritable()) {
                log.debug("[{}-{}] consumer is not writable. dispatching only 1 message to {} ", topic.getName(), name,
                        c);
            }
            int messagesForC = Math.min(
                    Math.min(entriesToDispatch, availablePermits),
                    serviceConfig.getDispatcherMaxRoundRobinBatchSize());

            if (messagesForC > 0) {

                // remove positions first from replay list first : sendMessages recycles entries
                if (readType == ReadType.Replay) {
                    entries.subList(start, start + messagesForC).forEach(entry -> {
                        messagesToRedeliver.remove(entry.getLedgerId(), entry.getEntryId());
                    });
                }

                SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                List<Entry> entriesForThisConsumer = entries.subList(start, start + messagesForC);

                EntryBatchSizes batchSizes = EntryBatchSizes.get(entriesForThisConsumer.size());
                EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entriesForThisConsumer.size());
                filterEntriesForConsumer(entriesForThisConsumer, batchSizes, sendMessageInfo, batchIndexesAcks, cursor,
                        transactionReader);

                c.sendMessages(entriesForThisConsumer, batchSizes, batchIndexesAcks, sendMessageInfo.getTotalMessages(),
                        sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(), redeliveryTracker);

                int msgSent = sendMessageInfo.getTotalMessages();
                start += messagesForC;
                entriesToDispatch -= messagesForC;
                TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, -(msgSent - batchIndexesAcks.getTotalAckedIndexCount()));
                totalMessagesSent += sendMessageInfo.getTotalMessages();
                totalBytesSent += sendMessageInfo.getTotalBytes();
            }
        }

        // acquire message-dispatch permits for already delivered messages
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            if (topic.getDispatchRateLimiter().isPresent()) {
                topic.getDispatchRateLimiter().get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
            }

            if (dispatchRateLimiter.isPresent()) {
                dispatchRateLimiter.get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
            }
        }

        if (entriesToDispatch > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No consumers found with available permits, storing {} positions for later replay", name,
                        entries.size() - start);
            }
            entries.subList(start, entries.size()).forEach(entry -> {
                messagesToRedeliver.add(entry.getLedgerId(), entry.getEntryId());
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
            if (cursor.getNumberOfEntriesInBacklog(false) == 0) {
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

                messagesToRedeliver.removeIf((ledgerId, entryId) -> {
                    return ComparisonChain.start().compare(ledgerId, markDeletePosition.getLedgerId())
                            .compare(entryId, markDeletePosition.getEntryId()).result() <= 0;
                });
            }
        }

        readBatchSize = serviceConfig.getDispatcherMinReadBatchSize();

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

    private boolean needTrimAckedMessages() {
        if (lastIndividualDeletedRangeFromCursorRecovery == null) {
            return false;
        } else {
            return lastIndividualDeletedRangeFromCursorRecovery.upperEndpoint()
                    .compareTo((PositionImpl) cursor.getReadPosition()) > 0;
        }
    }

    /**
     * returns true only if {@link consumerList} has atleast one unblocked consumer and have available permits
     *
     * @return
     */
    protected boolean isAtleastOneConsumerAvailable() {
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
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
        consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, none) -> {
            messagesToRedeliver.add(ledgerId, entryId);
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer,
                    messagesToRedeliver);
        }
        readMoreEntries();
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        positions.forEach(position -> {
            messagesToRedeliver.add(position.getLedgerId(), position.getEntryId());
            redeliveryTracker.addIfAbsent(position);
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer, positions);
        }
        readMoreEntries();
    }

    @Override
    public void addUnAckedMessages(int numberOfMessages) {
        int maxUnackedMessages = topic.getMaxUnackedMessagesOnSubscription();
        if (maxUnackedMessages == -1) {
            maxUnackedMessages = topic.getBrokerService().pulsar().getConfiguration()
                    .getMaxUnackedMessagesPerSubscription();
        }
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
    public void initializeDispatchRateLimiterIfNeeded(Optional<Policies> policies) {
        if (!dispatchRateLimiter.isPresent() && DispatchRateLimiter
                .isDispatchRateNeeded(topic.getBrokerService(), policies, topic.getName(), Type.SUBSCRIPTION)) {
            this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(topic, Type.SUBSCRIPTION));
        }
    }

    @Override
    public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
        if (!topic.isDelayedDeliveryEnabled()) {
            // If broker has the feature disabled, always deliver messages immediately
            return false;
        }

        synchronized (this) {
            if (!delayedDeliveryTracker.isPresent()) {
                // Initialize the tracker the first time we need to use it
                delayedDeliveryTracker = Optional
                        .of(topic.getBrokerService().getDelayedDeliveryTrackerFactory().newTracker(this));
            }

            delayedDeliveryTracker.get().resetTickTime(topic.getDelayedDeliveryTickTimeMillis());
            return delayedDeliveryTracker.get().addMessage(ledgerId, entryId, msgMetadata.getDeliverAtTime());
        }
    }

    protected synchronized Set<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        if (!messagesToRedeliver.isEmpty()) {
            return messagesToRedeliver.items(maxMessagesToRead,
                    (ledgerId, entryId) -> new PositionImpl(ledgerId, entryId));
        } else if (delayedDeliveryTracker.isPresent() && delayedDeliveryTracker.get().hasMessageAvailable()) {
            delayedDeliveryTracker.get().resetTickTime(topic.getDelayedDeliveryTickTimeMillis());
            return delayedDeliveryTracker.get().getScheduledMessages(maxMessagesToRead);
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public synchronized long getNumberOfDelayedMessages() {
        return delayedDeliveryTracker.map(DelayedDeliveryTracker::getNumberOfDelayedMessages).orElse(0L);
    }

    @Override
    public void cursorIsReset() {
        if (this.lastIndividualDeletedRangeFromCursorRecovery != null) {
            this.lastIndividualDeletedRangeFromCursorRecovery = null;
        }
    }

    public PersistentTopic getTopic() {
        return topic;
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherMultipleConsumers.class);
}
