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
import org.apache.pulsar.broker.systopic.TransactionBufferSnapshotSegmentSystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.matadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

public class SystemTopicBaseTxnBufferSnapshotSegmentService implements TransactionBufferSnapshotSegmentService {

    private final Map<TopicName, SystemTopicClient<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot>> clients;

    private final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    public SystemTopicBaseTxnBufferSnapshotSegmentService(PulsarClient client) {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot>> createWriter(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName).thenCompose(SystemTopicClient::newWriterAsync);
    }

    private CompletableFuture<SystemTopicClient<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot>>
    getTransactionBufferSystemTopicClient(
            TopicName topicName) {
        TopicName systemTopicName = NamespaceEventsSystemTopicFactory
                .getSystemTopicName(topicName.getNamespaceObject(), EventType.TRANSACTION_BUFFER_SNAPSHOT_SEGMENT);
        if (systemTopicName == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidTopicNameException(
                            "Can't create SystemTopicBaseTxnBufferSnapshotSegmentService, "
                                    + "because the topicName is null!"));
        }
        return CompletableFuture.completedFuture(clients.computeIfAbsent(systemTopicName,
                (v) -> namespaceEventsSystemTopicFactory
                        .createTransactionBufferSnapshotSegmentSystemTopicClient(topicName.getNamespaceObject(),
                                this)));
    }

    @Override
    public CompletableFuture<SystemTopicClient.Reader<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot>> createReader(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName).thenCompose(SystemTopicClient::newReaderAsync);
    }

    @Override
    public void removeClient(TopicName topicName,
                             TransactionBufferSnapshotSegmentSystemTopicClient
                                     transactionBufferSnapshotSegmentSystemTopicClient) {
        if (transactionBufferSnapshotSegmentSystemTopicClient.getReaders().size() == 0
                && transactionBufferSnapshotSegmentSystemTopicClient.getWriters().size() == 0) {
            clients.remove(topicName);
        }
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<TopicName, SystemTopicClient<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot>> entry : clients.entrySet()) {
            entry.getValue().close();
        }
    }
}
