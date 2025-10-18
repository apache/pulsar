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
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;

import static org.apache.pulsar.common.topics.TopicCompactionStrategy.TABLE_VIEW_TAG;

@Slf4j
class MessageTableViewImpl<T> implements TableView<Message<T>> {

    private final TableViewConfigurationData conf;

    private final ConcurrentMap<String, Message<T>> data;
    private final Map<String, Message<T>> immutableData;

    private final CompletableFuture<Reader<T>> reader;

    private final List<BiConsumer<String, Message<T>>> listeners;
    private final ReentrantLock listenersMutex;
    private final boolean isPersistentTopic;
    private TopicCompactionStrategy<Message<T>> compactionStrategy;

    private final ConcurrentHashMap<CompletableFuture<Void>, Map<String, TopicMessageId>> pendingRefreshRequests;

    private final ConcurrentHashMap<String, MessageId> lastReadPositions;

    MessageTableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf) {
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

    CompletableFuture<TableView<Message<T>>> start() {
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
    public Message<T> get(String key) {
       return data.get(key);
    }


    @Override
    public Set<Map.Entry<String, Message<T>>> entrySet() {
       return immutableData.entrySet();
    }

    @Override
    public Set<String> keySet() {
        return immutableData.keySet();
    }

    @Override
    public Collection<Message<T>> values() {
        return immutableData.values();
    }

    @Override
    public void forEach(BiConsumer<String, Message<T>> action) {
        data.forEach(action);
    }

    @Override
    public void listen(BiConsumer<String, Message<T>> action) {
        try {
            listenersMutex.lock();
            listeners.add(action);
        } finally {
            listenersMutex.unlock();
        }
    }

    @Override
    public void forEachAndListen(BiConsumer<String, Message<T>> action) {
        try {
            listenersMutex.lock();
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

        if (!msg.hasKey()) {
            log.warn("Received message with no key, releasing.");
            msg.release();
            checkAllFreshTask(msg);
            return;
        }

        String key = msg.getKey();

        Message<T> cur = msg.size() > 0 ? msg : null;

        if (log.isDebugEnabled()) {
            log.debug("Applying message from topic {}. key={} value={}",
                    conf.getTopicName(),
                    key,
                    cur);
        }

        boolean update = true;
        if (compactionStrategy != null) {
            Message<T> prev = data.get(key);
            update = !compactionStrategy.shouldKeepLeft(prev, cur);
            if (!update) {
                log.info("Skipped the message from topic {}. key={} value={} prev={}",
                        conf.getTopicName(),
                        key,
                        cur,
                        prev);
                compactionStrategy.handleSkippedMessage(key, cur);
                msg.release();
                checkAllFreshTask(msg);
                return;
            }
        }

        try {
            listenersMutex.lock();
            if (null == cur) {
                data.remove(key);
                msg.release();
            } else {
                data.put(key, cur);
            }

            for (BiConsumer<String, Message<T>> listener : listeners) {
                try {
                    listener.accept(key, cur);
                } catch (Throwable t) {
                    log.error("Table view listener raised an exception", t);
                }
            }
        } finally {
            listenersMutex.unlock();
        }

        checkAllFreshTask(msg);
    }

    @Override
    public CompletableFuture<Void> refreshAsync() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        reader.thenCompose(reader -> getLastMessageIdOfNonEmptyTopics(reader).thenAccept(lastMessageIds -> {
            if (lastMessageIds.isEmpty()) {
                completableFuture.complete(null);
                return;
            }
            pendingRefreshRequests.put(completableFuture, lastMessageIds);
            filterReceivedMessages(lastMessageIds);
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
                }
            });
            return lastMessageIdMap;
        });
    }

    private void filterReceivedMessages(Map<String, TopicMessageId> lastMessageIds) {
        lastMessageIds.forEach((partition, lastMessageId) -> {
            MessageId messageId = lastReadPositions.get(partition);
            if (messageId != null && lastMessageId.compareTo(messageId) <= 0) {
                lastMessageIds.remove(partition);
            }
        });
    }

    private boolean checkFreshTask(Map<String, TopicMessageId> maxMessageIds, CompletableFuture<Void> future,
                                   MessageId messageId, String topicName) {
        TopicMessageId maxMessageId = maxMessageIds.get(topicName);
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
                        pendingRefreshRequests.keySet().forEach(future -> {
                            pendingRefreshRequests.remove(future);
                            future.completeExceptionally(ex);
                        });
                    } else {
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