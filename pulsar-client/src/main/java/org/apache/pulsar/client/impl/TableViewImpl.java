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
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class TableViewImpl<T> implements TableView<T> {

    /**
     * The shortest delay before a tail read is retried, whatever the client's backoff settings: with a zero or
     * sub-millisecond initial backoff, {@link Backoff} gives 0 ms, and a read that keeps failing right away
     * would be retried back to back. It is the delay the retry used to sleep before it was given a backoff.
     */
    private static final long MIN_TAIL_READ_RETRY_DELAY_MS = 50;
    private final PulsarClientImpl client;
    private final TableViewConfigurationData conf;

    private final ConcurrentMap<String, T> data;
    private final Map<String, T> immutableData;

    private final CompletableFuture<Reader<T>> reader;

    private final List<BiConsumer<String, T>> listeners;
    private final ReentrantLock listenersMutex;
    private final boolean isPersistentTopic;
    private TopicCompactionStrategy<T> compactionStrategy;

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

    TableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf) {
        this.client = client;
        this.conf = conf;
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
                .poolMessages(true)
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

    CompletableFuture<TableView<T>> start() {
        return reader.thenCompose((reader) -> {
            if (!isPersistentTopic) {
                readTailMessages(reader);
                return CompletableFuture.completedFuture(null);
            }
            return this.readAllExistingMessages(reader)
                    .thenRun(() -> readTailMessages(reader));
        }).thenApply(__ -> this);
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
    public T get(String key) {
       return data.get(key);
    }

    @Override
    public Set<Map.Entry<String, T>> entrySet() {
       return immutableData.entrySet();
    }

    @Override
    public Set<String> keySet() {
        return immutableData.keySet();
    }

    @Override
    public Collection<T> values() {
        return immutableData.values();
    }

    @Override
    public void forEach(BiConsumer<String, T> action) {
        data.forEach(action);
    }

    @Override
    public void listen(BiConsumer<String, T> action) {
        try {
            listenersMutex.lock();
            listeners.add(action);
        } finally {
            listenersMutex.unlock();
        }
    }

    @Override
    public void forEachAndListen(BiConsumer<String, T> action) {
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
                String key = msg.getKey();
                T cur = msg.size() > 0 ? msg.getValue() : null;
                if (log.isDebugEnabled()) {
                    log.debug("Applying message from topic {}. key={} value={}",
                            conf.getTopicName(),
                            key,
                            cur);
                }

                boolean update = true;
                if (compactionStrategy != null) {
                    T prev = data.get(key);
                    update = !compactionStrategy.shouldKeepLeft(prev, cur);
                    if (!update) {
                        log.info("Skipped the message from topic {}. key={} value={} prev={}",
                                conf.getTopicName(),
                                key,
                                cur,
                                prev);
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
                        for (BiConsumer<String, T> listener : listeners) {
                            try {
                                listener.accept(key, cur);
                            } catch (Throwable t) {
                                log.error("Table view listener raised an exception", t);
                            }
                        }
                    } finally {
                        listenersMutex.unlock();
                    }
                }
            } else {
                // Keyless messages also advance the refresh position.
                lastReadPositions.put(msg.getTopicName(), msg.getMessageId());
            }
            checkAllFreshTask(msg);
        } finally {
            msg.release();
        }
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

    /**
     * Whether any refresh is still kept in {@link #activeRefreshes} or {@link #pendingRefreshRequests}. A
     * refresh is removed from both when it completes.
     */
    @VisibleForTesting
    boolean isTrackingRefreshes() {
        return !activeRefreshes.isEmpty() || !pendingRefreshRequests.isEmpty();
    }

    @Override
    public void refresh() throws PulsarClientException {
        try {
            refreshAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
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
                                       log.info("Reader {} was closed while reading existing messages.",
                                               reader.getTopic());
                                   } else {
                                       log.warn("Reader {} was interrupted while reading existing messages. ",
                                               reader.getTopic(), ex);
                                   }
                                   future.completeExceptionally(ex);
                                   return null;
                               });
                   } else {
                       // Reached the end
                       long endTime = System.nanoTime();
                       long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
                       log.info("Started table view for topic {} - Replayed {} messages in {} seconds",
                               reader.getTopic(),
                               messagesRead,
                               durationMillis / 1000.0);
                       future.complete(null);
                   }
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
                    // A successful read shows that the reader is healthy, so it ends the failure streak even if
                    // handling the message fails below. That message is consumed already and is not read again:
                    // growing the delay would only hold back the messages behind it.
                    tailReadBackoff.reset();
                    // Taken first: a pooled message is released once handled.
                    MessageId messageId = msg.getMessageId();
                    try {
                        handleMessage(msg);
                    } catch (Throwable t) {
                        log.warn("Reader {} failed to apply message {}, skipping it", reader.getTopic(), messageId,
                                t);
                        throw t;
                    }
                    readTailMessages(reader);
                }).exceptionally(ex -> {
                    if (read.isCompletedExceptionally()
                            && ex.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                        log.info("Reader {} was closed while reading tail messages.", reader.getTopic());
                        // No more messages can be read: fail the refreshes, whatever they are waiting for. Only a
                        // failed read says so: a handling failure carrying this exception, from a compaction
                        // strategy using some other closed resource for instance, is retried below.
                        stopTailReads(ex);
                    } else if (read.isCompletedExceptionally()
                            && ex.getCause() instanceof RejectedExecutionException) {
                        // The reader could not take the read: the executor it hands it to has been shut down,
                        // with the client or, when it is shared, under it. Like a closed reader, it reads
                        // nothing any more. A rejection thrown while handling a message is not this case and
                        // is retried below.
                        log.info("Reader {}'s executor was shut down while reading tail messages.",
                                reader.getTopic());
                        stopTailReads(alreadyClosed("The reader's executor was shut down"));
                    } else if (client.isClosed()) {
                        // A failure is not retried once the client is closed. The client closes its readers,
                        // but not one that had already failed for good, which would otherwise be retried for
                        // ever.
                        log.info("Client is closed, giving up retrying tail messages of reader {}.",
                                reader.getTopic());
                        stopTailReads(alreadyClosed("Client already closed"));
                    } else {
                        // Retry the other exceptions such as NotConnectedException after a backoff delay.
                        scheduleTailReadRetry(reader, ex, read.isCompletedExceptionally());
                    }
                    return null;
                });
    }

    /**
     * @param readFailed whether the read itself failed, rather than handling the message it returned; a handling
     *                   failure has been logged with its message already
     */
    private void scheduleTailReadRetry(Reader<T> reader, Throwable ex, boolean readFailed) {
        long delayMillis = Math.max(tailReadBackoff.next().toMillis(), MIN_TAIL_READ_RETRY_DELAY_MS);
        if (readFailed) {
            log.warn("Tail read of reader {} failed, retrying in {} ms", reader.getTopic(), delayMillis, ex);
        } else if (log.isDebugEnabled()) {
            log.debug("Resuming tail reads of reader {} in {} ms after a message could not be applied",
                    reader.getTopic(), delayMillis);
        }
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
