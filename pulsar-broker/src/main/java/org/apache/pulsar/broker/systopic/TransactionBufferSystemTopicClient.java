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

package org.apache.pulsar.broker.systopic;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotService;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class TransactionBufferSystemTopicClient extends SystemTopicClientBase<TransactionBufferSnapshot> {
    private TransactionBufferSnapshotService transactionBufferSnapshotService;

    public TransactionBufferSystemTopicClient(PulsarClient client, TopicName topicName,
                                              TransactionBufferSnapshotService transactionBufferSnapshotService) {
        super(client, topicName);
        this.transactionBufferSnapshotService = transactionBufferSnapshotService;
    }

    @Override
    protected CompletableFuture<Writer<TransactionBufferSnapshot>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(TransactionBufferSnapshot.class))
                .topic(topicName.toString())
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new transactionBufferSnapshot writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new TransactionBufferSnapshotWriter(producer, this));
                });
    }

    @Override
    protected CompletableFuture<Reader<TransactionBufferSnapshot>> newReaderAsyncInternal() {
        return client.newReader(Schema.AVRO(TransactionBufferSnapshot.class))
                .topic(topicName.toString())
                .startMessageId(MessageId.earliest)
                .readCompacted(true)
                .createAsync()
                .thenCompose(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new transactionBufferSnapshot buffer reader is created", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new TransactionBufferSnapshotReader(reader, this));
                });
    }

    protected void removeWriter(TransactionBufferSnapshotWriter writer) {
        writers.remove(writer);
        this.transactionBufferSnapshotService.removeClient(topicName, this);
    }

    protected void removeReader(TransactionBufferSnapshotReader reader) {
        readers.remove(reader);
        this.transactionBufferSnapshotService.removeClient(topicName, this);
    }

    private static class TransactionBufferSnapshotWriter implements Writer<TransactionBufferSnapshot> {

        private final Producer<TransactionBufferSnapshot> producer;
        private final TransactionBufferSystemTopicClient transactionBufferSystemTopicClient;

        private TransactionBufferSnapshotWriter(Producer<TransactionBufferSnapshot> producer,
                                                TransactionBufferSystemTopicClient transactionBufferSystemTopicClient) {
            this.producer = producer;
            this.transactionBufferSystemTopicClient = transactionBufferSystemTopicClient;
        }

        @Override
        public MessageId write(TransactionBufferSnapshot transactionBufferSnapshot) throws PulsarClientException {
            return producer.newMessage().key(transactionBufferSnapshot.getTopicName())
                    .value(transactionBufferSnapshot).send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(TransactionBufferSnapshot transactionBufferSnapshot) {
            return producer.newMessage().key(transactionBufferSnapshot.getTopicName())
                    .value(transactionBufferSnapshot).sendAsync();
        }

        @Override
        public MessageId delete(TransactionBufferSnapshot transactionBufferSnapshot) throws PulsarClientException {
            return producer.newMessage()
                    .key(transactionBufferSnapshot.getTopicName())
                    .value(null)
                    .send();
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(TransactionBufferSnapshot transactionBufferSnapshot) {
            return producer.newMessage()
                    .key(transactionBufferSnapshot.getTopicName())
                    .value(null)
                    .sendAsync();
        }

        @Override
        public void close() throws IOException {
            this.producer.close();
            transactionBufferSystemTopicClient.removeWriter(this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return producer.closeAsync().thenCompose(v -> {
                transactionBufferSystemTopicClient.removeWriter(this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<TransactionBufferSnapshot> getSystemTopicClient() {
            return transactionBufferSystemTopicClient;
        }
    }

    private static class TransactionBufferSnapshotReader implements Reader<TransactionBufferSnapshot> {

        private final org.apache.pulsar.client.api.Reader<TransactionBufferSnapshot> reader;
        private final TransactionBufferSystemTopicClient transactionBufferSystemTopicClient;

        private TransactionBufferSnapshotReader(org.apache.pulsar.client.api.Reader<TransactionBufferSnapshot> reader,
                                                TransactionBufferSystemTopicClient transactionBufferSystemTopicClient) {
            this.reader = reader;
            this.transactionBufferSystemTopicClient = transactionBufferSystemTopicClient;
        }

        @Override
        public Message<TransactionBufferSnapshot> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<TransactionBufferSnapshot>> readNextAsync() {
            return reader.readNextAsync();
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            return reader.hasMessageAvailable();
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            return reader.hasMessageAvailableAsync();
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
            transactionBufferSystemTopicClient.removeReader(this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return reader.closeAsync().thenCompose(v -> {
                transactionBufferSystemTopicClient.removeReader(this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<TransactionBufferSnapshot> getSystemTopic() {
            return transactionBufferSystemTopicClient;
        }
    }
}

