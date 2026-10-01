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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.topics.TopicCompactionStrategy.TABLE_VIEW_TAG;
import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Base class for {@link TableView} implementations. It reads messages of the schema type {@code T}
 * from the topic and maintains a map of the latest value of type {@code V} for each key.
 * Subclasses define how a message is converted into the value stored in the view.
 *
 * @param <T> the message schema type
 * @param <V> the type of the values stored in the view
 */
abstract class AbstractTableViewImpl<T, V> implements TableView<V> {

    private static final Logger LOG = Logger.get(AbstractTableViewImpl.class);
    protected final Logger log;
    private final PulsarClientImpl client;
    private final TableViewConfigurationData conf;

    private final ConcurrentMap<String, V> data;
    private final Map<String, V> immutableData;

    private final CompletableFuture<Reader<T>> reader;

    private final List<BiConsumer<String, V>> listeners;
    private final ReentrantLock listenersMutex;
    private final boolean isPersistentTopic;
    private final boolean poolMessages;
    private TopicCompactionStrategy<V> compactionStrategy;

    /**
     * Store the refresh tasks. When read to the position recording in the right map,
     * then remove the position in the right map. If the right map is empty, complete the future in the left.
     * There should be no timeout exception here, because the caller can only retry for TimeoutException.
     * It will only be completed exceptionally when no more messages can be read.
     */
    private final ConcurrentHashMap<CompletableFuture<Void>, Map<String, TopicMessageId>> pendingRefreshRequests;

    /**
     * This map stored the read position of each partition. It is used for the following case:
     * <p>
     *      1. Get last message ID.
     *      2. Receive message p1-1:1, p2-1:1, p2-1:2, p3-1:1
     *      3. Receive response of step1 {|p1-1:1|p2-2:2|p3-3:6|}
     *      4. No more messages are written to this topic.
     *      As a result, the refresh operation will never be completed.
     * </p>
     */
    private final ConcurrentHashMap<String, MessageId> lastReadPositions;

    /**
     * Backoff for retrying a failed tail read. A retry runs after the delay instead of sleeping on the thread
     * that completed the failed read, which is a shared client internal thread (or the caller's own thread
     * when the read fails immediately). Only the tail-read loop uses it, one step at a time, and each step
     * reaches the next thread through a future or an executor, so it needs no synchronization of its own.
     */
    private final Backoff tailReadBackoff;

    /**
     * Set when the tail-read loop stops: the table view is closing, or nothing can be read any more because the
     * reader was closed under the table view, its executor was shut down, or the client was closed. The first
     * cause is kept. The loop issues no read once it sees the cause, a retry still waiting for its delay does
     * nothing, and the refreshes that have not completed are failed with it, as is every refresh started
     * afterwards. It is shaped the way the failed read of a closed reader delivers it:
     * {@link PulsarClientException.AlreadyClosedException} in {@link Throwable#getCause()}, unwrapped by
     * {@code get()} as usual.
     */
    private final AtomicReference<Throwable> stopCause;

    /**
     * The refreshes that have not completed yet, from the moment {@link #refreshAsync()} is called, so that
     * stopping the tail reads can fail them whatever they are waiting for: the last message ids or a message.
     * A refresh is added here before it reads {@link #stopCause}, and {@link #stopTailReads(Throwable)} sets
     * the cause before it goes through this set, so one of the two always sees the other. A refresh leaves
     * this set, and {@link #pendingRefreshRequests}, when it completes.
     */
    private final Set<CompletableFuture<Void>> activeRefreshes;

    /**
     * @param poolMessages whether the reader should use pooled messages. When enabled, the handled messages
     *                     are released after they have been processed, so subclasses must not let the
     *                     message instance escape from {@link #getValue(Message)}.
     */
    AbstractTableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf,
                          boolean poolMessages) {
        this.client = client;
        this.conf = conf;
        this.log = LOG.with().attr("topic", conf.getTopicName()).build();
        this.poolMessages = poolMessages;
        this.isPersistentTopic = TopicName.get(conf.getTopicName()).getDomain() == TopicDomain.persistent;
        this.data = new ConcurrentHashMap<>();
        this.immutableData = Collections.unmodifiableMap(data);
        this.listeners = new ArrayList<>();
        this.listenersMutex = new ReentrantLock();
        this.compactionStrategy =
                TopicCompactionStrategy.load(TABLE_VIEW_TAG, conf.getTopicCompactionStrategyClassName());
        this.pendingRefreshRequests = new ConcurrentHashMap<>();
        this.lastReadPositions = new ConcurrentHashMap<>();
        this.tailReadBackoff = Backoff.builder()
                .initialDelay(Duration.ofNanos(client.getConfiguration().getInitialBackoffIntervalNanos()))
                .maxBackoff(Duration.ofNanos(client.getConfiguration().getMaxBackoffIntervalNanos()))
                .build();
        this.stopCause = new AtomicReference<>();
        this.activeRefreshes = ConcurrentHashMap.newKeySet();
        ReaderBuilder<T> readerBuilder = client.newReader(schema)
                .topic(conf.getTopicName())
                .startMessageId(MessageId.earliest)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval((int) conf.getAutoUpdatePartitionsSeconds(), TimeUnit.SECONDS)
                .poolMessages(poolMessages)
                .subscriptionName(conf.getSubscriptionName());
        if (isPersistentTopic) {
            readerBuilder.readCompacted(true);
        }

        CryptoKeyReader cryptoKeyReader = conf.getCryptoKeyReader();
        if (cryptoKeyReader != null) {
            readerBuilder.cryptoKeyReader(cryptoKeyReader);
        }

        readerBuilder.cryptoFailureAction(conf.getCryptoFailureAction());

        this.reader = readerBuilder.createAsync();
    }

    CompletableFuture<TableView<V>> start() {
        return reader.thenCompose((reader) -> {
            if (!isPersistentTopic) {
                readTailMessages(reader);
                return CompletableFuture.completedFuture(null);
            }
            return this.readAllExistingMessages(reader)
                    .thenRun(() -> readTailMessages(reader));
        }).<TableView<V>>thenApply(__ -> this).whenComplete((__, ex) -> {
            if (ex != null) {
                // Do not leak the reader when the initial replay fails
                closeAsync().exceptionally(closeEx -> null);
            }
        });
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    @Override
    public V get(String key) {
       return data.get(key);
    }

    @Override
    public Set<Map.Entry<String, V>> entrySet() {
       return immutableData.entrySet();
    }

    @Override
    public Set<String> keySet() {
        return immutableData.keySet();
    }

    @Override
    public Collection<V> values() {
        return immutableData.values();
    }

    @Override
    public void forEach(BiConsumer<String, V> action) {
        data.forEach(action);
    }

    @Override
    public void listen(BiConsumer<String, V> action) {
        try {
            listenersMutex.lock();
            listeners.add(action);
        } finally {
            listenersMutex.unlock();
        }
    }

    @Override
    public void forEachAndListen(BiConsumer<String, V> action) {
        // Ensure we iterate over all the existing entry _and_ start the listening from the exact next message
        try {
            listenersMutex.lock();

            // Execute the action over existing entries
            forEach(action);

            listeners.add(action);
        } finally {
            listenersMutex.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        stopTailReads(alreadyClosed("TableView was closed"));
        return reader.thenCompose(Reader::closeAsync);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    private void handleMessage(Message<T> msg) {
        try {
            if (msg.hasKey()) {
                handleKeyedMessage(msg);
            } else {
                // Keyless messages also advance the refresh position.
                lastReadPositions.put(msg.getTopicName(), msg.getMessageId());
            }
            checkAllFreshTask(msg);
        } finally {
            if (poolMessages) {
                msg.release();
            }
        }
    }

    private void handleKeyedMessage(Message<T> msg) {
        String key = msg.getKey();
        V cur;
        try {
            cur = getValueIfPresent(msg);
        } catch (Throwable t) {
            // The key keeps its previous value. A refresh must still observe this message as processed,
            // including when refreshAsync() is called from the error callback.
            lastReadPositions.put(msg.getTopicName(), msg.getMessageId());
            if (!onMappingError(msg, t)) {
                log.error().attr("key", key)
                        .attr("messageId", msg.getMessageId())
                        .exception(t)
                        .log("Skipping message whose value could not be decoded or mapped");
            }
            return;
        }
        log.debug().attr("key", key)
                .attr("value", cur)
                .log("Applying message");

        boolean update = true;
        if (compactionStrategy != null) {
            V prev = data.get(key);
            update = !compactionStrategy.shouldKeepLeft(prev, cur);
            if (!update) {
                log.info().attr("key", key)
                        .attr("value", cur)
                        .attr("prev", prev)
                        .log("Skipped the message");
                // The retained value is current before notifying the skipped-message callback.
                lastReadPositions.put(msg.getTopicName(), msg.getMessageId());
                compactionStrategy.handleSkippedMessage(key, cur);
            }
        }

        if (update) {
            try {
                listenersMutex.lock();
                if (null == cur) {
                    data.remove(key);
                } else {
                    data.put(key, cur);
                }

                // Refresh must see the updated table, including when called from a listener.
                lastReadPositions.put(msg.getTopicName(), msg.getMessageId());
                for (BiConsumer<String, V> listener : listeners) {
                    try {
                        listener.accept(key, cur);
                    } catch (Throwable t) {
                        log.error().exception(t).log("Table view listener raised an exception");
                    }
                }
            } finally {
                listenersMutex.unlock();
            }
        }
    }

    private V getValueIfPresent(Message<T> msg) throws Exception {
        return msg.size() > 0 ? getValue(msg) : null;
    }

    /**
     * Converts the message into the value stored in the view. Only called for messages with a non-empty
     * payload; messages with an empty payload are tombstones and remove the key from the view.
     * A {@code null} return value is also handled as a tombstone.
     *
     * @param msg the message to convert
     * @return the value to store in the view, or {@code null} to remove the key
     * @throws Exception if the message cannot be converted; the message is then skipped and
     *                   {@link #onMappingError(Message, Throwable)} is called
     */
    protected abstract V getValue(Message<T> msg) throws Exception;

    /**
     * Called when {@link #getValue(Message)} threw for a message that has been skipped.
     *
     * @return {@code true} if the failure has been handled, {@code false} to log it at ERROR level
     */
    protected boolean onMappingError(Message<T> msg, Throwable error) {
        return false;
    }

    @Override
    public CompletableFuture<Void> refreshAsync() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        // Registered before the stop cause is read, see activeRefreshes.
        activeRefreshes.add(completableFuture);
        completableFuture.whenComplete((result, error) -> {
            activeRefreshes.remove(completableFuture);
            pendingRefreshRequests.remove(completableFuture);
        });
        Throwable stopped = stopCause.get();
        if (stopped != null) {
            // No further tail read will be issued, so there is nothing to look up either.
            completableFuture.completeExceptionally(stopped);
            return completableFuture;
        }
        reader.thenCompose(reader -> getLastMessageIdOfNonEmptyTopics(reader).thenAccept(lastMessageIds -> {
            Throwable stoppedMeanwhile = stopCause.get();
            if (stoppedMeanwhile != null) {
                // The tail reads stopped during the lookup. stopTailReads() fails this refresh; an empty topic
                // must not turn that into a success.
                completableFuture.completeExceptionally(stoppedMeanwhile);
                return;
            }
            if (lastMessageIds.isEmpty()) {
                completableFuture.complete(null);
                return;
            }
            // After get the response of lastMessageIds, put the future and result into `refreshMap`
            // and then filter out partitions that has been read to the lastMessageID.
            pendingRefreshRequests.put(completableFuture, lastMessageIds);
            if (completableFuture.isDone()) {
                // Completed in the meantime, by stopTailReads() for example: its cleanup ran before this entry
                // existed.
                pendingRefreshRequests.remove(completableFuture);
                return;
            }
            filterReceivedMessages(lastMessageIds);
            // If there is no new messages, the refresh operation could be completed right now.
            if (lastMessageIds.isEmpty()) {
                pendingRefreshRequests.remove(completableFuture);
                completableFuture.complete(null);
            }
        })).exceptionally(throwable -> {
            completableFuture.completeExceptionally(throwable);
            pendingRefreshRequests.remove(completableFuture);
            return null;
        });
        return completableFuture;
    }

    @Override
    public void refresh() throws PulsarClientException {
        try {
            refreshAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    /**
     * Whether any refresh is still kept in {@link #activeRefreshes} or {@link #pendingRefreshRequests}. A
     * refresh is removed from both when it completes.
     */
    @VisibleForTesting
    boolean isTrackingRefreshes() {
        return !activeRefreshes.isEmpty() || !pendingRefreshRequests.isEmpty();
    }

    private CompletableFuture<Void> readAllExistingMessages(Reader<T> reader) {
        long startTime = System.nanoTime();
        AtomicLong messagesRead = new AtomicLong();

        CompletableFuture<Void> future = new CompletableFuture<>();
        getLastMessageIdOfNonEmptyTopics(reader).thenAccept(lastMessageIds -> {
            if (lastMessageIds.isEmpty()) {
                future.complete(null);
                return;
            }
            readAllExistingMessages(reader, future, startTime, messagesRead, lastMessageIds);
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });
        return future;
    }

    private CompletableFuture<Map<String, TopicMessageId>> getLastMessageIdOfNonEmptyTopics(Reader<T> reader) {
        return reader.getLastMessageIdsAsync().thenApply(lastMessageIds -> {
            Map<String, TopicMessageId> lastMessageIdMap = new ConcurrentHashMap<>();
            lastMessageIds.forEach(topicMessageId -> {
                if (((MessageIdAdv) topicMessageId).getEntryId() >= 0) {
                    lastMessageIdMap.put(topicMessageId.getOwnerTopic(), topicMessageId);
                } // else: a negative entry id represents an empty topic so that we don't have to read messages from it
            });
            return lastMessageIdMap;
        });
    }

    private void filterReceivedMessages(Map<String, TopicMessageId> lastMessageIds) {
        // The `lastMessageIds` and `readPositions` is concurrency-safe data types.
        lastMessageIds.forEach((partition, lastMessageId) -> {
            MessageId messageId = lastReadPositions.get(partition);
            if (messageId != null && lastMessageId.compareTo(messageId) <= 0) {
                lastMessageIds.remove(partition);
            }
        });
    }

    private boolean checkFreshTask(Map<String, TopicMessageId> maxMessageIds, CompletableFuture<Void> future,
                                   MessageId messageId, String topicName) {
        // The message received from multi-consumer/multi-reader is processed to TopicMessageImpl.
        TopicMessageId maxMessageId = maxMessageIds.get(topicName);
        // We need remove the partition from the maxMessageIds map
        // once the partition has been read completely.
        if (maxMessageId != null && messageId.compareTo(maxMessageId) >= 0) {
            maxMessageIds.remove(topicName);
        }
        if (maxMessageIds.isEmpty()) {
            future.complete(null);
            return true;
        } else {
            return false;
        }
    }

    private void checkAllFreshTask(Message<T> msg) {
        pendingRefreshRequests.forEach((future, maxMessageIds) -> {
            String topicName = msg.getTopicName();
            MessageId messageId = msg.getMessageId();
            if (checkFreshTask(maxMessageIds, future, messageId, topicName)) {
                pendingRefreshRequests.remove(future);
            }
        });
    }

    private void readAllExistingMessages(Reader<T> reader, CompletableFuture<Void> future, long startTime,
                                         AtomicLong messagesRead, Map<String, TopicMessageId> maxMessageIds) {
        reader.hasMessageAvailableAsync()
                .thenAccept(hasMessage -> {
                   if (hasMessage) {
                       reader.readNextAsync()
                               .thenAccept(msg -> {
                                  messagesRead.incrementAndGet();
                                  String topicName = msg.getTopicName();
                                  MessageId messageId = msg.getMessageId();
                                  handleMessage(msg);
                                  if (!checkFreshTask(maxMessageIds, future, messageId, topicName)) {
                                      readAllExistingMessages(reader, future, startTime,
                                              messagesRead, maxMessageIds);
                                  }
                               }).exceptionally(ex -> {
                                   if (ex.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                                       log.info().attr("reader", reader.getTopic())
                                               .log("Reader was closed while reading existing messages.");
                                   } else {
                                       log.warn().attr("reader", reader.getTopic())
                                               .exception(ex)
                                               .log("Reader was interrupted while reading existing messages.");
                                   }
                                   future.completeExceptionally(ex);
                                   return null;
                               });
                   } else {
                       // Reached the end
                       long endTime = System.nanoTime();
                       long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
                       log.info().attr("topic", reader.getTopic())
                               .attr("replayed", messagesRead)
                               .attr("durationSeconds", durationMillis / 1000.0)
                               .log("Started table view for topic - Replayed messages");
                       future.complete(null);
                   }
                }).exceptionally(ex -> {
                    // A failed hasMessageAvailableAsync() must fail the replay instead of leaving it pending
                    future.completeExceptionally(ex);
                    return null;
                });
    }

    private void readTailMessages(Reader<T> reader) {
        if (stopCause.get() != null) {
            // The tail reads have been stopped in the meantime.
            return;
        }
        // supplySafely(): a read that throws instead of returning a failed future is handled like a failed read.
        // Thrown from a retry, it would end this loop with no stop cause.
        CompletableFuture<Message<T>> read = FutureUtil.supplySafely(reader::readNextAsync);
        read.thenAccept(msg -> {
                    handleMessage(msg);
                    // Only a message that was read and handled ends the failure streak: a failure thrown while
                    // handling it lands in exceptionally() below and must keep backing off.
                    tailReadBackoff.reset();
                    readTailMessages(reader);
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                        log.info().attr("reader", reader.getTopic())
                                .log("Reader was closed while reading tail messages.");
                        // No more messages can be read: fail the refreshes, whatever they are waiting for.
                        stopTailReads(ex);
                    } else if (read.isCompletedExceptionally()
                            && ex.getCause() instanceof RejectedExecutionException) {
                        // The reader could not take the read: the executor it hands it to has been shut down,
                        // with the client or, when it is shared, under it. Like a closed reader, it reads
                        // nothing any more. A rejection thrown while handling a message is not this case and
                        // is retried below.
                        log.info().attr("reader", reader.getTopic())
                                .log("Reader's executor was shut down while reading tail messages.");
                        stopTailReads(alreadyClosed("The reader's executor was shut down"));
                    } else if (client.isClosed()) {
                        // A failure is not retried once the client is closed. The client closes its readers,
                        // but not one that had already failed for good, which would otherwise be retried for
                        // ever.
                        log.info().attr("reader", reader.getTopic())
                                .log("Client is closed, giving up retrying tail messages.");
                        stopTailReads(alreadyClosed("Client already closed"));
                    } else {
                        // Retry the other exceptions such as NotConnectedException after a backoff delay.
                        scheduleTailReadRetry(reader, ex);
                    }
                    return null;
                });
    }

    private void scheduleTailReadRetry(Reader<T> reader, Throwable ex) {
        long delayMillis = tailReadBackoff.next().toMillis();
        log.warn().attr("reader", reader.getTopic())
                .attr("retryDelayMs", delayMillis)
                .exception(ex)
                .log("Reader was interrupted while reading tail messages. Retrying..");
        // readTailMessages() checks the stop cause itself: a retry still waiting at a stop reads nothing.
        runAfterDelay(delayMillis, () -> readTailMessages(reader));
    }

    /**
     * Runs a tail-read retry once its delay has elapsed. The delay is not timed on one of the client's own
     * executors: a task queued there is dropped without notice when the executor is shut down, with the client
     * or, when it is shared, under it. No read is in flight while a retry waits, so the tail reads would end
     * there with nothing to record it, and the refreshes waiting for them would stay pending until the table
     * view is closed. The JDK's delayed executor does not depend on the client, so the retry still runs then,
     * outside the client's threads, and finds what became of the reader and of the client.
     */
    @VisibleForTesting
    void runAfterDelay(long delayMillis, Runnable retry) {
        CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS).execute(retry);
    }

    /**
     * Records why the tail-read loop stops and fails every refresh that has not completed. The cause is set
     * before the refreshes are looked up, see {@link #activeRefreshes}.
     */
    private void stopTailReads(Throwable cause) {
        stopCause.compareAndSet(null, cause);
        Throwable firstCause = stopCause.get();
        for (CompletableFuture<Void> refresh : activeRefreshes) {
            refresh.completeExceptionally(firstCause);
        }
    }

    /**
     * The failure of a refresh that can no longer complete, shaped like the one that used to reach it from the
     * closed reader's failed read: callbacks find the {@link PulsarClientException.AlreadyClosedException} in
     * {@link Throwable#getCause()}, and {@code get()} unwraps it as usual.
     */
    private static CompletionException alreadyClosed(String message) {
        return new CompletionException(new PulsarClientException.AlreadyClosedException(message));
    }
}
