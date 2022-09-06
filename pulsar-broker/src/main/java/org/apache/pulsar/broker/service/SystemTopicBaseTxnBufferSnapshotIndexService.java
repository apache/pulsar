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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClient.Reader;
import org.apache.pulsar.broker.systopic.SystemTopicClient.Writer;
import org.apache.pulsar.broker.systopic.TransactionBufferSnapshotIndexSystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.matadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.InvalidTopicNameException;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

public class SystemTopicBaseTxnBufferSnapshotIndexService implements TransactionBufferSnapshotIndexService {

    private final Map<TopicName, SystemTopicClient<TransactionBufferSnapshotIndexes>> clients;

    private final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    public SystemTopicBaseTxnBufferSnapshotIndexService(PulsarClient client) {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<Writer<TransactionBufferSnapshotIndexes>> createWriter(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName).thenCompose(SystemTopicClient::newWriterAsync);
    }

    private CompletableFuture<SystemTopicClient<TransactionBufferSnapshotIndexes>>
    getTransactionBufferSystemTopicClient(
            TopicName topicName) {
        TopicName systemTopicName = NamespaceEventsSystemTopicFactory
                .getSystemTopicName(topicName.getNamespaceObject(), EventType.TRANSACTION_BUFFER_SNAPSHOT);
        if (systemTopicName == null) {
            return FutureUtil.failedFuture(
                    new InvalidTopicNameException("Can't create SystemTopicBaseTxnBufferSnapshotIndexService, "
                            + "because the topicName is null!"));
        }
        return CompletableFuture.completedFuture(clients.computeIfAbsent(systemTopicName,
                (v) -> namespaceEventsSystemTopicFactory
                        .createTransactionBufferSnapshotIndexSystemTopicClient(topicName.getNamespaceObject(),
                                this)));
    }

    @Override
    public CompletableFuture<Reader<TransactionBufferSnapshotIndexes>> createReader(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName).thenCompose(SystemTopicClient::newReaderAsync);
    }

    @Override
    public void removeClient(TopicName topicName,
                             TransactionBufferSnapshotIndexSystemTopicClient
                                     transactionBufferSnapshotIndexSystemTopicClient) {
        if (transactionBufferSnapshotIndexSystemTopicClient.getReaders().size() == 0
                && transactionBufferSnapshotIndexSystemTopicClient.getWriters().size() == 0) {
            clients.remove(topicName);
        }
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<TopicName, SystemTopicClient<TransactionBufferSnapshotIndexes>> entry : clients.entrySet()) {
            entry.getValue().close();
        }
    }
}
