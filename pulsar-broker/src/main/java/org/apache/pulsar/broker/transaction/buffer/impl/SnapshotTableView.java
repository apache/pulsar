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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService;
import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.utils.SimpleCache;

/**
 * Compared with the more generic {@link org.apache.pulsar.client.api.TableView}, this table view
 * - Provides just a single public method that reads the latest value synchronously.
 * - Maintains multiple long-lived readers that will be expired after some time (1 minute by default).
 */
@Slf4j
public class SnapshotTableView {

    // Remove the cached reader and snapshots if there is no refresh request in 1 minute
    private static final long CACHE_EXPIRE_TIMEOUT_MS = 60 * 1000L;
    private final Map<String, TransactionBufferSnapshot> snapshots = new ConcurrentHashMap<>();
    private final SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshot> snapshotService;
    private final long blockTimeoutMs;
    private final SimpleCache<NamespaceName, Reader<TransactionBufferSnapshot>> readers;

    public SnapshotTableView(SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshot> snapshotService,
                             ScheduledExecutorService executor, long blockTimeoutMs) {
        this.snapshotService = snapshotService;
        this.blockTimeoutMs = blockTimeoutMs;
        this.readers = new SimpleCache<>(executor, CACHE_EXPIRE_TIMEOUT_MS);
    }

    public TransactionBufferSnapshot readLatest(String topic) throws Exception {
        final var topicName = TopicName.get(topic);
        final var namespace = topicName.getNamespaceObject();
        final var reader = readers.get(namespace, () -> {
            try {
                return wait(snapshotService.createReader(topicName), "create reader");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, __ -> __.closeAsync().exceptionally(e -> {
            log.warn("Failed to close reader {}", e.getMessage());
            return null;
        }));
        while (wait(reader.hasMoreEventsAsync(), "has more events")) {
            final var msg = wait(reader.readNextAsync(), "read message");
            if (msg.getKey() != null) {
                snapshots.put(msg.getKey(), msg.getValue());
            }
        }
        return snapshots.get(topic);
    }

    private <T> T wait(CompletableFuture<T> future, String msg) throws Exception {
        try {
            return future.get(blockTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new ExecutionException("Failed to " + msg, e.getCause());
        }
    }
}
