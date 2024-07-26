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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.apache.pulsar.broker.systopic.SystemTopicClient.Reader;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.utils.SimpleCache;

/**
 * Compared with the more generic {@link org.apache.pulsar.client.api.TableView}, this table view
 * - Provides just a single public method that reads the latest value synchronously.
 * - Maintains multiple long-lived readers that will be expired after some time (1 minute by default).
 */
@Slf4j
public class TableView<T> {

    // Remove the cached reader and snapshots if there is no refresh request in 1 minute
    private static final long CACHE_EXPIRE_TIMEOUT_MS = 60 * 1000L;
    private static final long CACHE_EXPIRE_CHECK_FREQUENCY_MS = 3000L;
    @VisibleForTesting
    protected final Function<TopicName, CompletableFuture<Reader<T>>> readerCreator;
    private final Map<String, T> snapshots = new ConcurrentHashMap<>();
    private final long clientOperationTimeoutMs;
    private final SimpleCache<NamespaceName, Reader<T>> readers;

    public TableView(Function<TopicName, CompletableFuture<Reader<T>>> readerCreator, long clientOperationTimeoutMs,
                     ScheduledExecutorService executor) {
        this.readerCreator = readerCreator;
        this.clientOperationTimeoutMs = clientOperationTimeoutMs;
        this.readers = new SimpleCache<>(executor, CACHE_EXPIRE_TIMEOUT_MS, CACHE_EXPIRE_CHECK_FREQUENCY_MS);
    }

    public T readLatest(String topic) throws Exception {
        final var reader = getReader(topic);
        while (wait(reader.hasMoreEventsAsync(), "has more events")) {
            final var msg = wait(reader.readNextAsync(), "read message");
            if (msg.getKey() != null) {
                if (msg.getValue() != null) {
                    snapshots.put(msg.getKey(), msg.getValue());
                } else {
                    snapshots.remove(msg.getKey());
                }
            }
        }
        return snapshots.get(topic);
    }

    @VisibleForTesting
    protected Reader<T> getReader(String topic) {
        final var topicName = TopicName.get(topic);
        return readers.get(topicName.getNamespaceObject(), () -> {
            try {
                return wait(readerCreator.apply(topicName), "create reader");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, __ -> __.closeAsync().exceptionally(e -> {
            log.warn("Failed to close reader {}", e.getMessage());
            return null;
        }));
    }

    private <R> R wait(CompletableFuture<R> future, String msg) throws Exception {
        try {
            return future.get(clientOperationTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new CompletionException("Failed to " + msg, e.getCause());
        }
    }
}
