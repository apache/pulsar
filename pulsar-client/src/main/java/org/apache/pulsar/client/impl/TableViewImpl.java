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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.pulsar.common.topics.TopicCompactionStrategy;

@Slf4j
public class TableViewImpl<T> implements TableView<T> {

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

    TableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf) {
        this.conf = conf;
        this.isPersistentTopic = conf.getTopicName().startsWith(TopicDomain.persistent.toString());
        this.data = new ConcurrentHashMap<>();
        this.immutableData = Collections.unmodifiableMap(data);
        this.listeners = new ArrayList<>();
        this.listenersMutex = new ReentrantLock();
        this.compactionStrategy =
                TopicCompactionStrategy.load(TABLE_VIEW_TAG, conf.getTopicCompactionStrategyClassName());
        this.pendingRefreshRequests = new ConcurrentHashMap<>();
        this.lastReadPositions = new ConcurrentHashMap<>();
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
        lastReadPositions.put(msg.getTopicName(), msg.getMessageId());
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
            }
            checkAllFreshTask(msg);
        } finally {
            msg.release();
        }
    }

    @Override
    public CompletableFuture<Void> refreshAsync() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        reader.thenCompose(reader -> getLastMessageIdOfNonEmptyTopics(reader).thenAccept(lastMessageIds -> {
            if (lastMessageIds.isEmpty()) {
                completableFuture.complete(null);
                return;
            }
            // After get the response of lastMessageIds, put the future and result into `refreshMap`
            // and then filter out partitions that has been read to the lastMessageID.
            pendingRefreshRequests.put(completableFuture, lastMessageIds);
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
        reader.readNextAsync()
                .thenAccept(msg -> {
                    handleMessage(msg);
                    readTailMessages(reader);
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                        log.info("Reader {} was closed while reading tail messages.", reader.getTopic());
                        // Fail all refresh request when no more messages can be read.
                        pendingRefreshRequests.keySet().forEach(future -> {
                            pendingRefreshRequests.remove(future);
                            future.completeExceptionally(ex);
                        });
                    } else {
                        // Retrying on the other exceptions such as NotConnectedException
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        log.warn("Reader {} was interrupted while reading tail messages. "
                                + "Retrying..", reader.getTopic(), ex);
                        readTailMessages(reader);
                    }
                    return null;
                });
    }
}
