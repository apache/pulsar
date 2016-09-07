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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ConcurrentFindCursorPositionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.yahoo.pulsar.broker.service.Consumer;
import com.yahoo.pulsar.broker.service.Dispatcher;
import com.yahoo.pulsar.broker.service.BrokerServiceException;
import com.yahoo.pulsar.broker.service.Subscription;
import com.yahoo.pulsar.broker.service.BrokerServiceException.PersistenceException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.SubscriptionFencedException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.SubscriptionInvalidCursorPosition;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.policies.data.ConsumerStats;
import com.yahoo.pulsar.common.policies.data.PersistentSubscriptionStats;
import com.yahoo.pulsar.common.util.Codec;
import com.yahoo.pulsar.utils.CopyOnWriteArrayList;

public class PersistentSubscription implements Subscription {
    private final PersistentTopic topic;
    private final ManagedCursor cursor;
    private volatile Dispatcher dispatcher;
    private final String topicName;
    private final String subName;

    private final AtomicBoolean isFenced = new AtomicBoolean(false);
    private PersistentMessageExpiryMonitor expiryMonitor;

    // for connected subscriptions, message expiry will be checked if the backlog is greater than this threshold
    private static final int MINIMUM_BACKLOG_FOR_EXPIRY_CHECK = 1000;

    public PersistentSubscription(PersistentTopic topic, ManagedCursor cursor) {
        this.topic = topic;
        this.cursor = cursor;
        this.topicName = topic.getName();
        this.subName = Codec.decode(cursor.getName());
        this.expiryMonitor = new PersistentMessageExpiryMonitor(topicName, cursor);
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        if (isFenced.get()) {
            log.warn("Attempting to add consumer {} on a fenced subscription", consumer);
            throw new SubscriptionFencedException("Subscription is fenced");
        }

        if (dispatcher == null || !dispatcher.isConsumerConnected()) {
            switch (consumer.subType()) {
            case Exclusive:
                if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
                    dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Exclusive, 0, topic);
                }
                break;
            case Shared:
                if (dispatcher == null || dispatcher.getType() != SubType.Shared) {
                    dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursor);
                }
                break;
            case Failover:
                int partitionIndex = DestinationName.getPartitionIndex(topicName);
                if (partitionIndex < 0) {
                    // For non partition topics, assume index 0 to pick a predictable consumer
                    partitionIndex = 0;
                }

                if (dispatcher == null || dispatcher.getType() != SubType.Failover) {
                    dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Failover, partitionIndex,
                            topic);
                }
                break;
            default:
                throw new ServerMetadataException("Unsupported subscription type");
            }
        } else {
            if (consumer.subType() != dispatcher.getType()) {
                throw new SubscriptionBusyException("Subscription is of different type");
            }
        }

        dispatcher.addConsumer(consumer);
        activateCursor();
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        if (dispatcher != null) {
            dispatcher.removeConsumer(consumer);
        }
        if (dispatcher.getConsumers().isEmpty()) {
            deactivateCursor();
        }

        // invalid consumer remove will throw an exception
        // decrement usage is triggered only for valid consumer close
        topic.usageCount.decrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                    topic.usageCount.get());
        }
    }

    public void deactivateCursor() {
        this.cursor.setInactive();
    }

    public void activateCursor() {
        this.cursor.setActive();
    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        dispatcher.consumerFlow(consumer, additionalNumberOfMessages);
    }

    @Override
    public void acknowledgeMessage(PositionImpl position, AckType ackType) {
        if (ackType == AckType.Cumulative) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Cumulative ack on {}", topicName, subName, position);
            }
            cursor.asyncMarkDelete(position, markDeleteCallback, position);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Individual ack on {}", topicName, subName, position);
            }
            cursor.asyncDelete(position, deleteCallback, position);
        }
    }

    private final MarkDeleteCallback markDeleteCallback = new MarkDeleteCallback() {
        @Override
        public void markDeleteComplete(Object ctx) {
            PositionImpl pos = (PositionImpl) ctx;
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Mark deleted messages until position {}", topicName, subName, pos);
            }
            pos.recycle();
        }

        @Override
        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
            // TODO: cut consumer connection on markDeleteFailed
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Failed to mark delete for position ", topicName, subName, ctx, exception);
            }
        }
    };

    private final DeleteCallback deleteCallback = new DeleteCallback() {
        @Override
        public void deleteComplete(Object ctx) {
            PositionImpl pos = (PositionImpl) ctx;
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Deleted message at {}", topicName, subName, pos);
            }

            pos.recycle();
        }

        @Override
        public void deleteFailed(ManagedLedgerException exception, Object ctx) {
            log.warn("[{}][{}] Failed to delete message at {}", topicName, subName, ctx, exception);
        }
    };

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("topic", topicName).add("name", subName).toString();
    }

    @Override
    public String getDestination() {
        return this.topicName;
    }

    public SubType getType() {
        return dispatcher != null ? dispatcher.getType() : null;
    }

    public String getTypeString() {
        SubType type = getType();
        if (type == null) {
            return "None";
        }

        switch (type) {
        case Exclusive:
            return "Exclusive";
        case Failover:
            return "Failover";
        case Shared:
            return "Shared";
        }

        return "Null";
    }

    @Override
    public CompletableFuture<Void> clearBacklog() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Backlog size before clearing: {}", topicName, subName,
                    cursor.getNumberOfEntriesInBacklog());
        }

        cursor.asyncClearBacklog(new ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Backlog size after clearing: {}", topicName, subName,
                            cursor.getNumberOfEntriesInBacklog());
                }
                future.complete(null);
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}][{}] Failed to clear backlog", topicName, subName, exception);
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    @Override
    public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Skipping {} messages, current backlog {}", topicName, subName, numMessagesToSkip,
                    cursor.getNumberOfEntriesInBacklog());
        }
        cursor.asyncSkipEntries(numMessagesToSkip, IndividualDeletedEntries.Exclude,
                new AsyncCallbacks.SkipEntriesCallback() {
                    @Override
                    public void skipEntriesComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] Skipped {} messages, new backlog {}", topicName, subName,
                                    numMessagesToSkip, cursor.getNumberOfEntriesInBacklog());
                        }
                        future.complete(null);
                    }

                    @Override
                    public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] Failed to skip {} messages", topicName, subName, numMessagesToSkip,
                                exception);
                        future.completeExceptionally(exception);
                    }
                }, null);

        return future;
    }

    @Override
    public CompletableFuture<Void> resetCursor(long timestamp) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (this) {
            if (dispatcher != null && dispatcher.isConsumerConnected()) {
                future.completeExceptionally(new SubscriptionBusyException("Subscription has active consumers"));
                return future;
            }
        }
        PersistentMessageFinder persistentMessageFinder = new PersistentMessageFinder(topicName, cursor);
        class Result {
            Position position = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Resetting subscription to timestamp {}", topicName, subName, timestamp);
        }
        persistentMessageFinder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                result.position = position;
                counter.countDown();
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Object ctx) {
                // todo - what can go wrong here that needs to be retried?
                if (exception instanceof ConcurrentFindCursorPositionException) {
                    future.completeExceptionally(new SubscriptionBusyException(exception.getMessage()));
                } else {
                    future.completeExceptionally(new BrokerServiceException(exception));
                }
                counter.countDown();
            }
        });

        try {
            counter.await();
        } catch (Exception e) {
            log.warn("[{}][{}] message find interrupted ", topicName, subName, e);
            if (future.isCompletedExceptionally()) {
                return future;
            }
            future.completeExceptionally(e);
            return future;
        }

        if (future.isCompletedExceptionally()) {
            return future;
        }
        if (result.position == null) {
            // this should not happen ideally unless a reset is requested for a time
            // that spans beyond the retention limits (time/size)
            result.position = cursor.getFirstPosition();
            if (result.position == null) {
                log.warn("[{}][{}] Unable to find position for timestamp {}. Unable to reset cursor to first position",
                        topicName, subName, timestamp);
                future.completeExceptionally(
                        new SubscriptionInvalidCursorPosition("Unable to find position for specified timestamp"));
                return future;
            }
            log.info(
                    "[{}][{}] Unable to find position for timestamp {}. Resetting cursor to first position {} in ledger",
                    topicName, subName, timestamp, result.position);
        }

        cursor.asyncResetCursor(result.position, new AsyncCallbacks.ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Successfully reset subscription to timestamp {}", topicName, subName,
                            timestamp);
                }
                future.complete(null);
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}][{}] Failed to reset subscription to timestamp {}", topicName, subName, timestamp,
                        exception);
                // todo - retry on InvalidCursorPositionException
                // or should we just ask user to retry one more time?
                if (exception instanceof InvalidCursorPositionException) {
                    future.completeExceptionally(new SubscriptionInvalidCursorPosition(exception.getMessage()));
                } else if (exception instanceof ConcurrentFindCursorPositionException) {
                    future.completeExceptionally(new SubscriptionBusyException(exception.getMessage()));
                } else {
                    future.completeExceptionally(new BrokerServiceException(exception));
                }
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Getting message at position {}", topicName, subName, messagePosition);
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
    public long getNumberOfEntriesInBacklog() {
        return cursor.getNumberOfEntriesInBacklog();
    }

    @Override
    public synchronized Dispatcher getDispatcher() {
        return this.dispatcher;
    }

    /**
     * Close the cursor ledger for this subscription. Requires that there are no active consumers on the dispatcher
     *
     * @return CompletableFuture indicating the completion of delete operation
     */
    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        synchronized (this) {
            if (dispatcher != null && dispatcher.isConsumerConnected()) {
                closeFuture.completeExceptionally(new SubscriptionBusyException("Subscription has active consumers"));
                return closeFuture;
            }
            isFenced.set(true);
            log.info("[{}][{}] Successfully fenced cursor ledger [{}]", topicName, subName, cursor);
        }

        cursor.asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Successfully closed cursor ledger", topicName, subName);
                }
                closeFuture.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                isFenced.set(false);

                log.error("[{}][{}] Error closing cursor for subscription", topicName, subName, exception);
                closeFuture.completeExceptionally(new PersistenceException(exception));
            }
        }, null);

        return closeFuture;
    }

    /**
     * Disconnect all consumers attached to the dispatcher and close this subscription
     *
     * @return CompletableFuture indicating the completion of disconnect operation
     */
    @Override
    public synchronized CompletableFuture<Void> disconnect() {
        CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();

        // block any further consumers on this subscription
        isFenced.set(true);

        (dispatcher != null ? dispatcher.disconnect() : CompletableFuture.completedFuture(null))
                .thenCompose(v -> close()).thenRun(() -> {
                    log.info("[{}][{}] Successfully disconnected and closed subscription", topicName, subName);
                    disconnectFuture.complete(null);
                }).exceptionally(exception -> {
                    isFenced.set(false);

                    log.error("[{}][{}] Error disconnecting consumers from subscription", topicName, subName,
                            exception);
                    disconnectFuture.completeExceptionally(exception);
                    return null;
                });

        return disconnectFuture;
    }

    /**
     * Delete the subscription by closing and deleting its managed cursor if no consumers are connected to it. Handle
     * unsubscribe call from admin layer.
     *
     * @return CompletableFuture indicating the completion of delete operation
     */
    @Override
    public CompletableFuture<Void> delete() {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

        // cursor close handles pending delete (ack) operations
        this.close().thenCompose(v -> topic.unsubscribe(subName)).thenAccept(v -> deleteFuture.complete(null))
                .exceptionally(exception -> {
                    isFenced.set(false);
                    log.error("[{}][{}] Error deleting subscription", topicName, subName, exception);
                    deleteFuture.completeExceptionally(exception);
                    return null;
                });

        return deleteFuture;
    }

    /**
     * Handle unsubscribe command from the client API Check with the dispatcher is this consumer can proceed with
     * unsubscribe
     *
     * @param consumer
     *            consumer object that is initiating the unsubscribe operation
     * @return CompletableFuture indicating the completion of ubsubscribe operation
     */
    @Override
    public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            if (dispatcher.canUnsubscribe(consumer)) {
                consumer.close();
                return delete();
            }
            future.completeExceptionally(
                    new ServerMetadataException("Unconnected or shared consumer attempting to unsubscribe"));
        } catch (BrokerServiceException e) {
            log.warn("Error removing consumer {}", consumer);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CopyOnWriteArrayList<Consumer> getConsumers() {
        Dispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            return dispatcher.getConsumers();
        } else {
            return CopyOnWriteArrayList.empty();
        }
    }

    @Override
    public void expireMessages(int messageTTLInSeconds) {
        if ((getNumberOfEntriesInBacklog() == 0) || (dispatcher != null && dispatcher.isConsumerConnected()
                && getNumberOfEntriesInBacklog() < MINIMUM_BACKLOG_FOR_EXPIRY_CHECK
                && !topic.isOldestMessageExpired(cursor, messageTTLInSeconds))) {
            // don't do anything for almost caught-up connected subscriptions
            return;
        }
        expiryMonitor.expireMessages(messageTTLInSeconds);
    }

    public double getExpiredMessageRate() {
        return expiryMonitor.getMessageExpiryRate();
    }

    public PersistentSubscriptionStats getStats() {
        PersistentSubscriptionStats subStats = new PersistentSubscriptionStats();

        Dispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            dispatcher.getConsumers().forEach(consumer -> {
                ConsumerStats consumerStats = consumer.getStats();
                subStats.consumers.add(consumerStats);
                subStats.msgRateOut += consumerStats.msgRateOut;
                subStats.msgThroughputOut += consumerStats.msgThroughputOut;
            });
        }

        subStats.msgBacklog = getNumberOfEntriesInBacklog();
        subStats.msgRateExpired = expiryMonitor.getMessageExpiryRate();
        subStats.type = getType();
        return subStats;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
        dispatcher.redeliverUnacknowledgedMessages(consumer);
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentSubscription.class);
}
