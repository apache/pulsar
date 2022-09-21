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
import org.apache.pulsar.broker.systopic.SystemTopicClientBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;

public abstract class SystemTopicTxnBufferSnapshotBaseService<T> implements SystemTopicTxnBufferSnapshotService<T> {

    protected final Map<TopicName, SystemTopicClient<T>> clients;
    protected final NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    public SystemTopicTxnBufferSnapshotBaseService(PulsarClient client) {
        this.namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(client);
        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<SystemTopicClient.Writer<T>> createWriter(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName).thenCompose(SystemTopicClient::newWriterAsync);
    }

    @Override
    public CompletableFuture<SystemTopicClient.Reader<T>> createReader(TopicName topicName) {
        return getTransactionBufferSystemTopicClient(topicName).thenCompose(SystemTopicClient::newReaderAsync);
    }

    @Override
    public void removeClient(TopicName topicName, SystemTopicClientBase<T> transactionBufferSystemTopicClient) {
        if (transactionBufferSystemTopicClient.getReaders().size() == 0
                && transactionBufferSystemTopicClient.getWriters().size() == 0) {
            clients.remove(topicName);
        }
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<TopicName, SystemTopicClient<T>> entry : clients.entrySet()) {
            entry.getValue().close();
        }
    }

    protected abstract CompletableFuture<SystemTopicClient<T>> getTransactionBufferSystemTopicClient(
            TopicName topicName);

}
