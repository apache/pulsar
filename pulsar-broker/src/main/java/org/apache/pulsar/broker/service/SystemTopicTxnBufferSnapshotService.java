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
package org.apache.pulsar.broker.service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClientBase;
import org.apache.pulsar.broker.transaction.buffer.impl.TableView;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class SystemTopicTxnBufferSnapshotService<T> {

    protected final ConcurrentHashMap<NamespaceName, SystemTopicClient<T>> clients;
    protected final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    protected final Class<T> schemaType;
    protected final EventType systemTopicType;

    private final ConcurrentHashMap<NamespaceName, ReferenceCountedWriter<T>> refCountedWriterMap;
    @Getter
    private final TableView<T> tableView;

    // The class ReferenceCountedWriter will maintain the reference count,
    // when the reference count decrement to 0, it will be removed from writerFutureMap, the writer will be closed.
    public static class ReferenceCountedWriter<T> {

        private final AtomicLong referenceCount;
        private final NamespaceName namespaceName;
        private final CompletableFuture<SystemTopicClient.Writer<T>> future;
        private final SystemTopicTxnBufferSnapshotService<T> snapshotService;

        public ReferenceCountedWriter(NamespaceName namespaceName,
                                      CompletableFuture<SystemTopicClient.Writer<T>> future,
                                      SystemTopicTxnBufferSnapshotService<T> snapshotService) {
            this.referenceCount = new AtomicLong(1);
            this.namespaceName = namespaceName;
            this.snapshotService = snapshotService;
            this.future = future;
            this.future.exceptionally(t -> {
                        log.error("[{}] Failed to create TB snapshot writer.", namespaceName, t);
                snapshotService.refCountedWriterMap.remove(namespaceName, this);
                return null;
            });
        }

        public CompletableFuture<SystemTopicClient.Writer<T>> getFuture() {
            return future;
        }

        private synchronized boolean retain() {
            return this.referenceCount.incrementAndGet() > 0;
        }

        public synchronized void release() {
            if (this.referenceCount.decrementAndGet() == 0) {
                snapshotService.refCountedWriterMap.remove(namespaceName, this);
                future.thenAccept(writer -> {
                    final String topicName = writer.getSystemTopicClient().getTopicName().toString();
                    writer.closeAsync().exceptionally(t -> {
                        if (t != null) {
                            log.error("[{}] Failed to close TB snapshot writer.", topicName, t);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Success to close TB snapshot writer.", topicName);
                            }
                        }
                        return null;
                    });
                });
            }
        }

    }

    public SystemTopicTxnBufferSnapshotService(PulsarService pulsar, EventType systemTopicType,
                                               Class<T> schemaType) throws PulsarServerException {
        final var client = (PulsarClientImpl) pulsar.getClient();
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.systemTopicType = systemTopicType;
        this.schemaType = schemaType;
        this.clients = new ConcurrentHashMap<>();
        this.refCountedWriterMap = new ConcurrentHashMap<>();
        this.tableView = new TableView<>(this::createReader,
                client.getConfiguration().getOperationTimeoutMs(), pulsar.getExecutor());
    }

    public CompletableFuture<SystemTopicClient.Reader<T>> createReader(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName.getNamespaceObject()).newReaderAsync();
    }

    public void removeClient(TopicName topicName, SystemTopicClientBase<T> transactionBufferSystemTopicClient) {
        if (transactionBufferSystemTopicClient.getReaders().size() == 0
                && transactionBufferSystemTopicClient.getWriters().size() == 0) {
            clients.remove(topicName.getNamespaceObject());
        }
    }

    public ReferenceCountedWriter<T> getReferenceWriter(NamespaceName namespaceName) {
        return refCountedWriterMap.compute(namespaceName, (k, v) -> {
            if (v != null && v.retain()) {
                return v;
            } else {
                return new ReferenceCountedWriter<>(namespaceName,
                        getTransactionBufferSystemTopicClient(namespaceName).newWriterAsync(), this);
            }
        });
    }

    private SystemTopicClient<T> getTransactionBufferSystemTopicClient(NamespaceName namespaceName) {
        TopicName systemTopicName = NamespaceEventsSystemTopicFactory
                .getSystemTopicName(namespaceName, systemTopicType);
        if (systemTopicName == null) {
            throw new RuntimeException(new PulsarClientException.InvalidTopicNameException(
                    "Can't get the TB system topic client for namespace " + namespaceName
                            + " with type " + systemTopicType + "."));
        }

        return clients.computeIfAbsent(namespaceName,
                (v) -> namespaceEventsSystemTopicFactory
                        .createTransactionBufferSystemTopicClient(systemTopicName, this, schemaType));
    }

    public void close() throws Exception {
        for (Map.Entry<NamespaceName, SystemTopicClient<T>> entry : clients.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                log.error("Failed to close system topic client for namespace {}", entry.getKey(), e);
            }
        }
        clients.clear();
        for (Map.Entry<NamespaceName, ReferenceCountedWriter<T>> entry : refCountedWriterMap.entrySet()) {
            CompletableFuture<SystemTopicClient.Writer<T>> future = entry.getValue().getFuture();
            if (!future.isCompletedExceptionally()) {
                future.thenAccept(writer -> {
                    try {
                        writer.close();
                    } catch (Exception e) {
                        log.error("Failed to close writer for namespace {}", entry.getKey(), e);
                    }
                });
            }
        }
        refCountedWriterMap.clear();
    }

}
