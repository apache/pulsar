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

package org.apache.pulsar.client.impl;

import io.netty.util.Timeout;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class TableViewImpl<T> implements TableView<T> {

    private final PulsarClientImpl client;
    private final Schema<T> schema;
    private final TableViewConfigurationData conf;

    private final ConcurrentMap<String, T> data;
    private final Map<String, T> immutableData;

    private final ConcurrentMap<String, Reader<T>> readers;

    private final List<BiConsumer<String, T>> listeners;
    private final ReentrantLock listenersMutex;

    TableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf) {
        this.client = client;
        this.schema = schema;
        this.conf = conf;
        this.data = new ConcurrentHashMap<>();
        this.immutableData = Collections.unmodifiableMap(data);
        this.readers = new ConcurrentHashMap<>();
        this.listeners = new ArrayList<>();
        this.listenersMutex = new ReentrantLock();
    }

    CompletableFuture<TableView<T>> start() {
        return client.getPartitionsForTopic(conf.getTopicName())
                .thenCompose(partitions -> {
                    Set<String> partitionsSet = new HashSet<>(partitions);
                    List<CompletableFuture<?>> futures = new ArrayList<>();

                    // Add new Partitions
                    partitions.forEach(partition -> {
                        if (!readers.containsKey(partition)) {
                            futures.add(newReader(partition));
                        }
                    });

                    // Remove partitions that are not used anymore
                    readers.forEach((existingPartition, existingReader) -> {
                        if (!partitionsSet.contains(existingPartition)) {
                            futures.add(existingReader.closeAsync()
                                    .thenRun(() -> readers.remove(existingPartition, existingReader)));
                        }
                    });

                    return FutureUtil.waitForAll(futures)
                            .thenRun(() -> schedulePartitionsCheck());
                }).thenApply(__ -> this);
    }

    private void schedulePartitionsCheck() {
        client.timer()
                .newTimeout(this::checkForPartitionsChanges, conf.getAutoUpdatePartitionsSeconds(), TimeUnit.SECONDS);
    }

    private void checkForPartitionsChanges(Timeout timeout) {
        if (timeout.isCancelled()) {
            return;
        }

        start().whenComplete((tw, ex) -> {
           if (ex != null) {
               log.warn("Failed to check for changes in number of partitions: {}", ex);
               schedulePartitionsCheck();
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
        return FutureUtil.waitForAll(
                readers.values().stream()
                        .map(Reader::closeAsync)
                        .collect(Collectors.toList())
        );
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
                if (log.isDebugEnabled()) {
                    log.debug("Applying message from topic {}. key={} value={}",
                            conf.getTopicName(),
                            msg.getKey(),
                            msg.getValue());
                }

                try {
                    listenersMutex.lock();
                    if (null == msg.getValue()){
                        data.remove(msg.getKey());
                    } else {
                        data.put(msg.getKey(), msg.getValue());
                    }

                    for (BiConsumer<String, T> listener : listeners) {
                        try {
                            listener.accept(msg.getKey(), msg.getValue());
                        } catch (Throwable t) {
                            log.error("Table view listener raised an exception", t);
                        }
                    }
                } finally {
                    listenersMutex.unlock();
                }
            }
        } finally {
            msg.release();
        }
    }

    private CompletableFuture<Reader<T>> newReader(String partition) {
        return client.newReader(schema)
                .topic(partition)
                .startMessageId(MessageId.earliest)
                .readCompacted(true)
                .poolMessages(true)
                .createAsync()
                .thenCompose(this::cacheNewReader)
                .thenCompose(this::readAllExistingMessages);
    }

    private CompletableFuture<Reader<T>> cacheNewReader(Reader<T> reader) {
        CompletableFuture<Reader<T>> future = new CompletableFuture<>();
        if (this.readers.containsKey(reader.getTopic())) {
            future.completeExceptionally(
                    new IllegalArgumentException("reader on partition " + reader.getTopic() + " already existed"));
        } else {
            this.readers.put(reader.getTopic(), reader);
            future.complete(reader);
        }

        return future;
    }

    private CompletableFuture<Reader<T>> readAllExistingMessages(Reader<T> reader) {
        long startTime = System.nanoTime();
        AtomicLong messagesRead = new AtomicLong();

        CompletableFuture<Reader<T>> future = new CompletableFuture<>();
        readAllExistingMessages(reader, future, startTime, messagesRead);
        return future;
    }

    private void readAllExistingMessages(Reader<T> reader, CompletableFuture<Reader<T>> future, long startTime,
                                         AtomicLong messagesRead) {
        reader.hasMessageAvailableAsync()
                .thenAccept(hasMessage -> {
                   if (hasMessage) {
                       reader.readNextAsync()
                               .thenAccept(msg -> {
                                  messagesRead.incrementAndGet();
                                  handleMessage(msg);
                                  readAllExistingMessages(reader, future, startTime, messagesRead);
                               }).exceptionally(ex -> {
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
                       future.complete(reader);
                       readTailMessages(reader);
                   }
                });
    }

    private void readTailMessages(Reader<T> reader) {
        reader.readNextAsync()
                .thenAccept(msg -> {
                    handleMessage(msg);
                    readTailMessages(reader);
                }).exceptionally(ex -> {
                    log.info("Reader {} was interrupted", reader.getTopic());
                    return null;
                });
    }
}
