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

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClient.Reader;
import org.apache.pulsar.broker.systopic.SystemTopicClient.Writer;
import org.apache.pulsar.broker.systopic.TransactionBufferSystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class SystemTopicBaseTxnBufferSnapshotService implements TransactionBufferSnapshotService {

    private final Map<NamespaceName, SystemTopicClient<TransactionBufferSnapshot>> clients;

    private final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    private final ConcurrentHashMap<NamespaceName, ReferenceCountedWriter> writerFutureMap;
    private final LinkedList<CompletableFuture<Writer<TransactionBufferSnapshot>>> pendingCloseWriterList;

    // The class ReferenceCountedWriter will maintain the reference count,
    // when the reference count decrement to 0, it will be removed from writerFutureMap, the writer will be closed.
    public static class ReferenceCountedWriter extends AbstractReferenceCounted {

        private final NamespaceName namespaceName;
        private final SystemTopicBaseTxnBufferSnapshotService service;
        private final CompletableFuture<Writer<TransactionBufferSnapshot>> future;

        private ReferenceCountedWriter(NamespaceName namespaceName,
                                         SystemTopicBaseTxnBufferSnapshotService service,
                                         CompletableFuture<Writer<TransactionBufferSnapshot>> future) {
            this.namespaceName = namespaceName;
            this.service = service;
            this.future = future;
            this.future.exceptionally(t -> {
                log.error("[{}] Failed to create transaction buffer snapshot writer.", namespaceName, t);
                service.writerFutureMap.remove(namespaceName, this);
                return null;
            });
        }

        public CompletableFuture<Writer<TransactionBufferSnapshot>> getFuture() {
            return future;
        }

        @Override
        protected void deallocate() {
            if (service.writerFutureMap.remove(namespaceName, this)) {
                service.pendingCloseWriterList.add(this.future);
                service.closePendingCloseWriter();
            }
        }

        @Override
        public ReferenceCounted touch(Object o) {
            return this;
        }

    }

    public SystemTopicBaseTxnBufferSnapshotService(PulsarClient client) {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.clients = new ConcurrentHashMap<>();
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
        ReferenceCountedWriter referenceCountedWriter = writerFutureMap.compute(namespaceName, (ns, value) -> {
            if (value == null) {
                return new ReferenceCountedWriter(namespaceName, this,
                        getTransactionBufferSystemTopicClient(namespaceName).newWriterAsync());
            }
            try {
                value.retain();
            } catch (Exception e) {
                log.warn("[{}] The reference counted writer was already released, {}", namespaceName, e.getMessage());
                return null;
            }
            return value;
        });
        if (referenceCountedWriter == null) {
            // normally, this will not happen, just a safety check.
            throw new RuntimeException(
                    String.format("[%s] Failed to create transaction buffer snapshot writer.", namespaceName));
        }
        return referenceCountedWriter;
    }

    @Override
    public void releaseReferenceWriter(ReferenceCountedWriter referenceCountedWriter) {
        synchronized (writerFutureMap) {
            ReferenceCountUtil.safeRelease(referenceCountedWriter);
        }
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
