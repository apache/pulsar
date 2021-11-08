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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.NonDurableCursorImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDispatcherSingleActiveConsumer extends AbstractBaseDispatcher {

    protected final String topicName;
    protected static final AtomicReferenceFieldUpdater<AbstractDispatcherSingleActiveConsumer, Consumer>
            ACTIVE_CONSUMER_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
            AbstractDispatcherSingleActiveConsumer.class, Consumer.class, "activeConsumer");
    private volatile Consumer activeConsumer = null;
    protected final CopyOnWriteArrayList<Consumer> consumers;
    protected StickyKeyConsumerSelector stickyKeyConsumerSelector;
    protected boolean isKeyHashRangeFiltered = false;
    protected CompletableFuture<Void> closeFuture = null;
    protected final int partitionIndex;
    protected final ManagedCursor cursor;
    // This dispatcher supports both the Exclusive and Failover subscription types
    protected final SubType subscriptionType;

    protected static final int FALSE = 0;
    protected static final int TRUE = 1;
    protected static final AtomicIntegerFieldUpdater<AbstractDispatcherSingleActiveConsumer> IS_CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractDispatcherSingleActiveConsumer.class, "isClosed");
    private volatile int isClosed = FALSE;

    protected boolean isFirstRead = true;

    public AbstractDispatcherSingleActiveConsumer(SubType subscriptionType, int partitionIndex,
                                                  String topicName, Subscription subscription,
                                                  ServiceConfiguration serviceConfig, ManagedCursor cursor) {
        super(subscription, serviceConfig);
        this.topicName = topicName;
        this.consumers = new CopyOnWriteArrayList<>();
        this.partitionIndex = partitionIndex;
        this.subscriptionType = subscriptionType;
        this.cursor = cursor;
        ACTIVE_CONSUMER_UPDATER.set(this, null);
    }

    protected abstract void scheduleReadOnActiveConsumer();

    protected abstract void readMoreEntries(Consumer consumer);

    protected abstract void cancelPendingRead();

    protected void notifyActiveConsumerChanged(Consumer activeConsumer) {
        if (null != activeConsumer && subscriptionType == SubType.Failover) {
            consumers.forEach(consumer ->
                consumer.notifyActiveConsumerChange(activeConsumer));
        }
    }

    /**
     * Pick active consumer for a topic for {@link SubType#Failover} subscription.
     * If it's a non-partitioned topic then it'll pick consumer based on order they subscribe to the topic.
     * If is's a partitioned topic, first sort consumers based on their priority level and consumer name then
     * distributed partitions evenly across consumers with highest priority level.
     *
     * @return the true consumer if the consumer is changed, otherwise false.
     */
    protected boolean pickAndScheduleActiveConsumer() {
        checkArgument(!consumers.isEmpty());
        // By default always pick the first connected consumer for non partitioned topic.
        int index = 0;

        // If it's a partitioned topic, sort consumers based on priority level then consumer name.
        if (partitionIndex >= 0) {
            AtomicBoolean hasPriorityConsumer = new AtomicBoolean(false);
            consumers.sort((c1, c2) -> {
                int priority = c1.getPriorityLevel() - c2.getPriorityLevel();
                if (priority != 0) {
                    hasPriorityConsumer.set(true);
                    return priority;
                }
                return c1.consumerName().compareTo(c2.consumerName());
            });

            int consumersSize = consumers.size();
            // find number of consumers which are having the highest priorities. so partitioned-topic assignment happens
            // evenly across highest priority consumers
            if (hasPriorityConsumer.get()) {
                int highestPriorityLevel = consumers.get(0).getPriorityLevel();
                for (int i = 0; i < consumers.size(); i++) {
                    if (highestPriorityLevel != consumers.get(i).getPriorityLevel()) {
                        consumersSize = i;
                        break;
                    }
                }
            }
            index = partitionIndex % consumersSize;
        }

        Consumer prevConsumer = ACTIVE_CONSUMER_UPDATER.getAndSet(this, consumers.get(index));

        Consumer activeConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
        if (prevConsumer == activeConsumer) {
            // Active consumer did not change. Do nothing at this point
            return false;
        } else {
            // If the active consumer is changed, send notification.
            scheduleReadOnActiveConsumer();
            return true;
        }
    }

    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer {}", this.topicName, consumer);
            consumer.disconnect();
        }

        if (subscriptionType == SubType.Exclusive && !consumers.isEmpty()) {
            throw new ConsumerBusyException("Exclusive consumer is already connected");
        }

        if (subscriptionType == SubType.Failover && isConsumersExceededOnSubscription()) {
            log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit",
                    this.topicName);
            throw new ConsumerBusyException("Subscription reached max consumers limit");
        }

        if (subscriptionType == SubType.Exclusive
                && consumer.getKeySharedMeta() != null
                && consumer.getKeySharedMeta().getHashRangesList() != null
                && consumer.getKeySharedMeta().getHashRangesList().size() > 0) {
            stickyKeyConsumerSelector = new HashRangeExclusiveStickyKeyConsumerSelector();
            stickyKeyConsumerSelector.addConsumer(consumer);
            isKeyHashRangeFiltered = true;
        } else {
            isKeyHashRangeFiltered = false;
        }

        if (consumers.isEmpty()) {
            isFirstRead = true;
        }

        consumers.add(consumer);

        if (!pickAndScheduleActiveConsumer()) {
            // the active consumer is not changed
            Consumer currentActiveConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
            if (null == currentActiveConsumer) {
                if (log.isDebugEnabled()) {
                    log.debug("Current active consumer disappears while adding consumer {}", consumer);
                }
            } else {
                consumer.notifyActiveConsumerChange(currentActiveConsumer);
            }
        }
        if (cursor != null && !cursor.isDurable() && cursor instanceof NonDurableCursorImpl) {
            ((NonDurableCursorImpl) cursor).setReadCompacted(ACTIVE_CONSUMER_UPDATER.get(this).readCompacted());
        }

    }

    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        log.info("Removing consumer {}", consumer);
        if (!consumers.remove(consumer)) {
            throw new ServerMetadataException("Consumer was not connected");
        }

        if (consumers.isEmpty()) {
            ACTIVE_CONSUMER_UPDATER.set(this, null);
        }

        if (closeFuture == null && !consumers.isEmpty()) {
            pickAndScheduleActiveConsumer();
            return;
        }

        cancelPendingRead();

        if (consumers.isEmpty() && closeFuture != null && !closeFuture.isDone()) {
            // Control reaches here only when closeFuture is created
            // and no more connected consumers left.
            closeFuture.complete(null);
        }
    }

    /**
     * Handle unsubscribe command from the client API For failover subscription, if consumer is connected consumer, we
     * can unsubscribe.
     *
     * @param consumer
     *            Calling consumer object
     */
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return (consumers.size() == 1) && Objects.equals(consumer, ACTIVE_CONSUMER_UPDATER.get(this));
    }

    public CompletableFuture<Void> close() {
        IS_CLOSED_UPDATER.set(this, TRUE);
        return disconnectAllConsumers();
    }

    public boolean isClosed() {
        return isClosed == TRUE;
    }

    /**
     * Disconnect all consumers on this dispatcher (server side close). This triggers channelInactive on the inbound
     * handler which calls dispatcher.removeConsumer(), where the closeFuture is completed
     *
     * @return
     */
    public synchronized CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
        closeFuture = new CompletableFuture<>();

        if (!consumers.isEmpty()) {
            consumers.forEach(consumer -> consumer.disconnect(isResetCursor));
            cancelPendingRead();
        } else {
            // no consumer connected, complete disconnect immediately
            closeFuture.complete(null);
        }
        return closeFuture;
    }

    public synchronized CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
        closeFuture = new CompletableFuture<>();
        if (activeConsumer != null) {
            activeConsumer.disconnect(isResetCursor);
        }
        closeFuture.complete(null);
        return closeFuture;
    }

    @Override
    public synchronized void resetCloseFuture() {
        closeFuture = null;
    }

    public void reset() {
        resetCloseFuture();
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    public SubType getType() {
        return subscriptionType;
    }

    public Consumer getActiveConsumer() {
        return ACTIVE_CONSUMER_UPDATER.get(this);
    }

    @Override
    public List<Consumer> getConsumers() {
        return consumers;
    }

    public boolean isConsumerConnected() {
        return ACTIVE_CONSUMER_UPDATER.get(this) != null;
    }

    private static final Logger log = LoggerFactory.getLogger(AbstractDispatcherSingleActiveConsumer.class);

}
