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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClient.Reader;
import org.apache.pulsar.broker.systopic.SystemTopicClient.Writer;
import org.apache.pulsar.broker.systopic.TransactionBufferSystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class SystemTopicBaseTxnBufferSnapshotService implements TransactionBufferSnapshotService {

    private final Map<NamespaceName, SystemTopicClient<TransactionBufferSnapshot>> clients;

    private final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentHashMap<NamespaceName, ReferenceCountedWriter> writerFutureMap;
    private final LinkedList<CompletableFuture<Writer<TransactionBufferSnapshot>>> pendingCloseWriterList;

    // The class ReferenceCountedWriter will maintain the reference count,
    // when the reference count decrement to 0, it will be removed from writerFutureMap, the writer will be closed.
    public static class ReferenceCountedWriter extends AbstractReferenceCounted {

        private final NamespaceName namespaceName;
        private final SystemTopicBaseTxnBufferSnapshotService service;
        private CompletableFuture<Writer<TransactionBufferSnapshot>> future;
        private final Backoff backoff;

        protected ReferenceCountedWriter(NamespaceName namespaceName,
                                         SystemTopicBaseTxnBufferSnapshotService service) {
            this.namespaceName = namespaceName;
            this.service = service;
            this.backoff = new Backoff(1, TimeUnit.SECONDS, 3, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);
            initWriterFuture();
        }

        private synchronized void initWriterFuture() {
            this.future = service.getTransactionBufferSystemTopicClient(namespaceName).newWriterAsync();
            this.future.thenRunAsync(this.backoff::reset).exceptionally(throwable -> {
                long delay = backoff.next();
                log.error("[{}] Failed to new transaction buffer system topic writer," +
                                "try to re-create the writer in {} ms.", delay, namespaceName, throwable);
                service.scheduledExecutorService.schedule(
                        SafeRun.safeRun(this::initWriterFuture), delay, TimeUnit.MILLISECONDS);
                return null;
            });
        }

        public CompletableFuture<Writer<TransactionBufferSnapshot>> getFuture() {
            if (future == null) {
                // normally, this will not happen, not affect reference count, only avoid return a null object.
                initWriterFuture();
            }
            return future;
        }

        @Override
        protected void deallocate() {
            service.writerFutureMap.compute(namespaceName, (k, v) -> {
                if (v == this) {
                    // only remove it's self, avoid remove new add reference count object
                    service.writerFutureMap.remove(namespaceName);
                    service.pendingCloseWriterList.add(this.future);
                    service.closePendingCloseWriter();
                    return null;
                }
                return v;
            });
        }

        @Override
        public ReferenceCounted touch(Object o) {
            return this;
        }

    }

    public SystemTopicBaseTxnBufferSnapshotService(PulsarClient client,
                                                   ScheduledExecutorService scheduledExecutorService) {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.clients = new ConcurrentHashMap<>();
        this.scheduledExecutorService = scheduledExecutorService;
        this.writerFutureMap = new ConcurrentHashMap<>();
        this.pendingCloseWriterList = new LinkedList<>();
    }

    @Override
    public CompletableFuture<Writer<TransactionBufferSnapshot>> createWriter(TopicName topicName) {
        if (topicName == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidTopicNameException(
                            "Can't create SystemTopicBaseTxnBufferSnapshotService, because the topicName is null!"));
        }
        return getTransactionBufferSystemTopicClient(topicName.getNamespaceObject()).newWriterAsync();
    }

    @Override
    public ReferenceCountedWriter createReferenceWriter(NamespaceName namespaceName) {
        return writerFutureMap.compute(namespaceName, (ns, writerFuture) -> {
            if (writerFuture == null) {
                return new ReferenceCountedWriter(namespaceName, this);
            }
            try {
                writerFuture.retain();
            } catch (Exception e) {
                // Resolve potential race condition problem, if retain method encounter reference count exception
                // or other exceptions, create a new `ReferenceCountedWriter`, when the `ReferenceCountedWriter` release
                // but didn't remove from `writerFutureMap`.
                return new ReferenceCountedWriter(namespaceName, this);
            }
            return writerFuture;
        });
    }

    private SystemTopicClient<TransactionBufferSnapshot> getTransactionBufferSystemTopicClient(
            NamespaceName namespaceName) {
        return clients.computeIfAbsent(namespaceName,
                (v) -> namespaceEventsSystemTopicFactory
                        .createTransactionBufferSystemTopicClient(namespaceName, this));
    }

    @Override
    public CompletableFuture<Reader<TransactionBufferSnapshot>> createReader(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName.getNamespaceObject()).newReaderAsync();
    }

    @Override
    public void removeClient(TopicName topicName,
                                          TransactionBufferSystemTopicClient transactionBufferSystemTopicClient) {
        if (transactionBufferSystemTopicClient.getReaders().size() == 0
                && transactionBufferSystemTopicClient.getWriters().size() == 0) {
            clients.remove(topicName.getNamespaceObject());
        }
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<NamespaceName, SystemTopicClient<TransactionBufferSnapshot>> entry : clients.entrySet()) {
            entry.getValue().close();
        }
    }

    private void closePendingCloseWriter() {
        Iterator<CompletableFuture<Writer<TransactionBufferSnapshot>>> iterator =
                pendingCloseWriterList.stream().iterator();
        while (iterator.hasNext()) {
            CompletableFuture<Writer<TransactionBufferSnapshot>> future = iterator.next();
            if (future == null) {
                continue;
            }
            future.thenAccept(writer ->
                    writer.closeAsync().thenAccept(ignore ->
                            pendingCloseWriterList.remove(future)));
        }
    }

}
