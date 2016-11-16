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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.service.BrokerServiceException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import com.yahoo.pulsar.broker.service.Consumer;
import com.yahoo.pulsar.broker.service.Dispatcher;
import com.yahoo.pulsar.client.impl.Backoff;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.utils.CopyOnWriteArrayList;

public final class PersistentDispatcherSingleActiveConsumer implements Dispatcher, ReadEntriesCallback {

    private final PersistentTopic topic;
    private final ManagedCursor cursor;
    private final AtomicReference<Consumer> activeConsumer = new AtomicReference<Consumer>();
    private final CopyOnWriteArrayList<Consumer> consumers;
    private boolean havePendingRead = false;
    private CompletableFuture<Void> closeFuture = null;
    private final int partitionIndex;

    // This dispatcher supports both the Exclusive and Failover subscription types
    private final SubType subscriptionType;

    private static final int MaxReadBatchSize = 100;
    private int readBatchSize;
    private final Backoff readFailureBackoff = new Backoff(15, TimeUnit.SECONDS, 1, TimeUnit.MINUTES);

    public PersistentDispatcherSingleActiveConsumer(ManagedCursor cursor, SubType subscriptionType, int partitionIndex,
            PersistentTopic topic) {
        this.topic = topic;
        this.cursor = cursor;
        this.consumers = new CopyOnWriteArrayList<>();
        this.partitionIndex = partitionIndex;
        this.subscriptionType = subscriptionType;
        this.readBatchSize = MaxReadBatchSize;
    }

    private void pickAndScheduleActiveConsumer() {
        checkArgument(!consumers.isEmpty());

        consumers.sort((c1, c2) -> c1.consumerName().compareTo(c2.consumerName()));

        int index = partitionIndex % consumers.size();
        Consumer prevConsumer = activeConsumer.getAndSet(consumers.get(index));

        if (prevConsumer == activeConsumer.get()) {
            // Active consumer did not change. Do nothing at this point
            return;
        }

        if (havePendingRead && cursor.cancelPendingReadRequest()) {
            havePendingRead = false;
        }

        // When a new consumer is chosen, start delivery from unacked message. If there is any pending read operation,
        // let it finish and then rewind
        if (!havePendingRead) {
            cursor.rewind();
            readMoreEntries(activeConsumer.get());
        }
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        if (subscriptionType == SubType.Exclusive && !consumers.isEmpty()) {
            throw new ConsumerBusyException("Exclusive consumer is already connected");
        }

        consumers.add(consumer);

        // Pick an active consumer and start it
        pickAndScheduleActiveConsumer();

    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        log.info("Removing consumer {}", consumer);
        if (!consumers.remove(consumer)) {
            throw new ServerMetadataException("Consumer was not connected");
        }

        if (consumers.isEmpty()) {
            activeConsumer.set(null);
        }

        if (closeFuture == null && !consumers.isEmpty()) {
            pickAndScheduleActiveConsumer();
            return;
        }

        if (havePendingRead && cursor.cancelPendingReadRequest()) {
            havePendingRead = false;
        }

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
    @Override
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return (consumers.size() == 1) && Objects.equals(consumer, activeConsumer.get());
    }

    /**
     * Disconnect all consumers on this dispatcher (server side close). This triggers channelInactive on the inbound
     * handler which calls dispatcher.removeConsumer(), where the closeFuture is completed
     *
     * @return
     */
    @Override
    public synchronized CompletableFuture<Void> disconnect() {
        closeFuture = new CompletableFuture<>();

        if (!consumers.isEmpty()) {
            consumers.forEach(Consumer::disconnect);
            if (havePendingRead && cursor.cancelPendingReadRequest()) {
                havePendingRead = false;
            }
        } else {
            // no consumer connected, complete disconnect immediately
            closeFuture.complete(null);
        }
        return closeFuture;
    }

    @Override
    public synchronized void readEntriesComplete(final List<Entry> entries, Object obj) {
        Consumer readConsumer = (Consumer) obj;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Got messages: {}", readConsumer, entries.size());
        }

        havePendingRead = false;

        if (readBatchSize < MaxReadBatchSize) {
            int newReadBatchSize = Math.min(readBatchSize * 2, MaxReadBatchSize);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Increasing read batch size from {} to {}", readConsumer, readBatchSize,
                        newReadBatchSize);
            }

            readBatchSize = newReadBatchSize;
        }

        readFailureBackoff.reduceToHalf();

        Consumer currentConsumer = activeConsumer.get();
        if (currentConsumer == null || readConsumer != currentConsumer) {
            // Active consumer has changed since the read request has been issued. We need to rewind the cursor and
            // re-issue the read request for the new consumer
            entries.forEach(Entry::release);
            cursor.rewind();
            if (currentConsumer != null) {
                readMoreEntries(currentConsumer);
            }
        } else {
            currentConsumer.sendMessages(entries).getLeft().addListener(future -> {
                if (future.isSuccess()) {
                    // Schedule a new read batch operation only after the previous batch has been written to the socket
                    synchronized (PersistentDispatcherSingleActiveConsumer.this) {
                        Consumer newConsumer = activeConsumer.get();
                        if (newConsumer != null && !havePendingRead) {
                            readMoreEntries(newConsumer);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "[{}] Ignoring write future complete. consumerAvailable={} havePendingRead={}",
                                        newConsumer, newConsumer != null, havePendingRead);
                            }
                        }
                    }
                }
            });
        }
    }

    @Override
    public synchronized void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        if (!havePendingRead) {
            if (activeConsumer.get() == consumer) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Trigger new read after receiving flow control message", consumer);
                }
                readMoreEntries(consumer);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Ignoring flow control message since consumer is not active partition consumer",
                            consumer);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring flow control message since we already have a pending read req", consumer);
            }
        }
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
        if (consumer != activeConsumer.get()) {
            log.info("[{}] Ignoring reDeliverUnAcknowledgedMessages: Only the active consumer can call resend",
                    consumer);
            return;
        }
        if (havePendingRead && cursor.cancelPendingReadRequest()) {
            havePendingRead = false;
        }
        if (!havePendingRead) {
            cursor.rewind();
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cursor rewinded, redelivering unacknowledged messages. ", consumer);
            }
            readMoreEntries(consumer);
        } else {
            log.info("[{}] Ignoring reDeliverUnAcknowledgedMessages: cancelPendingRequest on cursor failed", consumer);
        }

    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        // We cannot redeliver single messages to single consumers to preserve ordering.
        redeliverUnacknowledgedMessages(consumer);
    }

    private void readMoreEntries(Consumer consumer) {
        int availablePermits = consumer.getAvailablePermits();

        if (availablePermits > 0) {
            if (!consumer.isWritable()) {
                // If the connection is not currently writable, we issue the read request anyway, but for a single
                // message. The intent here is to keep use the request as a notification mechanism while avoiding to
                // read and dispatch a big batch of messages which will need to wait before getting written to the
                // socket.
                availablePermits = 1;
            }

            int messagesToRead = Math.min(availablePermits, readBatchSize);

            // Schedule read
            if (log.isDebugEnabled()) {
                log.debug("[{}] Schedule read of {} messages", consumer, messagesToRead);
            }
            havePendingRead = true;
            cursor.asyncReadEntriesOrWait(messagesToRead, this, consumer);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer buffer is full, pause reading", consumer);
            }
        }
    }

    @Override
    public synchronized void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

        havePendingRead = false;
        Consumer c = (Consumer) ctx;

        long waitTimeMillis = readFailureBackoff.next();

        if (!(exception instanceof TooManyRequestsException)) {
            log.error("[{}] Error reading entries at {} : {} - Retrying to read in {} seconds", c,
                    cursor.getReadPosition(), exception.getMessage(), waitTimeMillis / 1000.0);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got throttled by bookies while reading at {} : {} - Retrying to read in {} seconds", c,
                        cursor.getReadPosition(), exception.getMessage(), waitTimeMillis / 1000.0);
            }
        }

        checkNotNull(c);

        // Reduce read batch size to avoid flooding bookies with retries
        readBatchSize = 1;

        topic.getBrokerService().executor().schedule(() -> {
            synchronized (PersistentDispatcherSingleActiveConsumer.this) {
                Consumer currentConsumer = activeConsumer.get();
                // we should retry the read if we have an active consumer and there is no pending read
                if (currentConsumer != null && !havePendingRead) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Retrying read operation", c);
                    }
                    readMoreEntries(currentConsumer);
                } else {
                    log.info("[{}] Skipping read retry: Current Consumer {}, havePendingRead {}", c, currentConsumer,
                            havePendingRead);
                }
            }
        }, waitTimeMillis, TimeUnit.MILLISECONDS);

    }

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherSingleActiveConsumer.class);

    @Override
    public boolean isConsumerConnected() {
        return activeConsumer.get() != null;
    }

    @Override
    public CopyOnWriteArrayList<Consumer> getConsumers() {
        return consumers;
    }

    @Override
    public SubType getType() {
        return subscriptionType;
    }

    public Consumer getActiveConsumer() {
        return activeConsumer.get();
    }

}
