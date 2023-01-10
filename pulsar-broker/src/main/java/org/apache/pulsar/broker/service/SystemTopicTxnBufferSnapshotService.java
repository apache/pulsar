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
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClientBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

public class SystemTopicTxnBufferSnapshotService<T> {

    protected final Map<TopicName, SystemTopicClient<T>> clients;
    protected final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    protected final Class<T> schemaType;
    protected final EventType systemTopicType;

    public SystemTopicTxnBufferSnapshotService(PulsarClient client, EventType systemTopicType,
                                               Class<T> schemaType) {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.systemTopicType = systemTopicType;
        this.schemaType = schemaType;
        this.clients = new ConcurrentHashMap<>();
    }

    public CompletableFuture<SystemTopicClient.Writer<T>> createWriter(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName).thenCompose(SystemTopicClient::newWriterAsync);
    }

    public CompletableFuture<SystemTopicClient.Reader<T>> createReader(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName).thenCompose(SystemTopicClient::newReaderAsync);
    }

    public void removeClient(TopicName topicName, SystemTopicClientBase<T> transactionBufferSystemTopicClient) {
        if (transactionBufferSystemTopicClient.getReaders().size() == 0
                && transactionBufferSystemTopicClient.getWriters().size() == 0) {
            clients.remove(topicName);
        }
    }

    protected CompletableFuture<SystemTopicClient<T>> getTransactionBufferSystemTopicClient(TopicName topicName) {
        TopicName systemTopicName = NamespaceEventsSystemTopicFactory
                .getSystemTopicName(topicName.getNamespaceObject(), systemTopicType);
        if (systemTopicName == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException
                            .InvalidTopicNameException("Can't create SystemTopicBaseTxnBufferSnapshotIndexService, "
                            + "because the topicName is null!"));
        }
        return CompletableFuture.completedFuture(clients.computeIfAbsent(systemTopicName,
                (v) -> namespaceEventsSystemTopicFactory
                        .createTransactionBufferSystemTopicClient(systemTopicName,
                                this, schemaType)));
    }

    public void close() throws Exception {
        for (Map.Entry<TopicName, SystemTopicClient<T>> entry : clients.entrySet()) {
            entry.getValue().close();
        }
    }

}
