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
package org.apache.pulsar.broker.systopic;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class  TransactionBufferSnapshotBaseSystemTopicClient<T> extends SystemTopicClientBase<T> {

    protected final SystemTopicTxnBufferSnapshotService<T> systemTopicTxnBufferSnapshotService;
    protected final Class<T> schemaType;

    public TransactionBufferSnapshotBaseSystemTopicClient(PulsarClient client,
                                                          TopicName topicName,
                                                          SystemTopicTxnBufferSnapshotService<T>
                                                                  systemTopicTxnBufferSnapshotService,
                                                          Class<T> schemaType) {
        super(client, topicName);
        this.systemTopicTxnBufferSnapshotService = systemTopicTxnBufferSnapshotService;
        this.schemaType = schemaType;
    }

    protected void removeWriter(Writer<T> writer) {
        writers.remove(writer);
        this.systemTopicTxnBufferSnapshotService.removeClient(topicName, this);
    }

    protected void removeReader(Reader<T> reader) {
        readers.remove(reader);
        this.systemTopicTxnBufferSnapshotService.removeClient(topicName, this);
    }

    protected static class TransactionBufferSnapshotWriter<T> implements Writer<T> {

        protected final Producer<T> producer;
        protected final TransactionBufferSnapshotBaseSystemTopicClient<T>
                transactionBufferSnapshotBaseSystemTopicClient;

        protected TransactionBufferSnapshotWriter(Producer<T> producer,
                                                  TransactionBufferSnapshotBaseSystemTopicClient<T>
                                                    transactionBufferSnapshotBaseSystemTopicClient) {
            this.producer = producer;
            this.transactionBufferSnapshotBaseSystemTopicClient = transactionBufferSnapshotBaseSystemTopicClient;
        }

        @Override
        public MessageId write(String key, T t)
                throws PulsarClientException {
            return producer.newMessage().key(key)
                    .value(t).send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(String key, T t) {
            return producer.newMessage()
                    .key(key)
                    .value(t).sendAsync();
        }

        @Override
        public MessageId delete(String key, T t)
                throws PulsarClientException {
            return producer.newMessage()
                    .key(key)
                    .value(null)
                    .send();
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(String key, T t) {
            return producer.newMessage()
                    .key(key)
                    .value(null)
                    .sendAsync();
        }

        @Override
        public void close() throws IOException {
            this.closeAsync().join();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            producer.closeAsync().whenComplete((v, e) -> {
                // if close fail, also need remove the producer
                transactionBufferSnapshotBaseSystemTopicClient.removeWriter(this);
                if (e != null) {
                    completableFuture.completeExceptionally(e);
                    return;
                }
                completableFuture.complete(null);
            });
            return completableFuture;
        }

        @Override
        public SystemTopicClient<T> getSystemTopicClient() {
            return transactionBufferSnapshotBaseSystemTopicClient;
        }
    }

    protected static class TransactionBufferSnapshotReader<T> implements Reader<T> {

        private final org.apache.pulsar.client.api.Reader<T> reader;
        private final TransactionBufferSnapshotBaseSystemTopicClient<T> transactionBufferSnapshotBaseSystemTopicClient;

        protected TransactionBufferSnapshotReader(
                org.apache.pulsar.client.api.Reader<T> reader,
                TransactionBufferSnapshotBaseSystemTopicClient<T> transactionBufferSnapshotBaseSystemTopicClient) {
            this.reader = reader;
            this.transactionBufferSnapshotBaseSystemTopicClient = transactionBufferSnapshotBaseSystemTopicClient;
        }

        @Override
        public Message<T> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<T>> readNextAsync() {
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
            this.closeAsync().join();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            reader.closeAsync().whenComplete((v, e) -> {
                // if close fail, also need remove the reader
                transactionBufferSnapshotBaseSystemTopicClient.removeReader(this);
                if (e != null) {
                    completableFuture.completeExceptionally(e);
                    return;
                }
                completableFuture.complete(null);
            });
            return completableFuture;
        }

        @Override
        public SystemTopicClient<T> getSystemTopic() {
            return transactionBufferSnapshotBaseSystemTopicClient;
        }
    }

    @Override
    protected CompletableFuture<Writer<T>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(schemaType))
                .topic(topicName.toString())
                .enableBatching(false)
                .createAsync().thenApply(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new {} writer is created", topicName, schemaType.getName());
                    }
                    return  new TransactionBufferSnapshotWriter<>(producer, this);
                });
    }

    @Override
    protected CompletableFuture<Reader<T>> newReaderAsyncInternal() {
        return client.newReader(Schema.AVRO(schemaType))
                .topic(topicName.toString())
                .startMessageId(MessageId.earliest)
                .readCompacted(true)
                .createAsync()
                .thenApply(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new {} reader is created", topicName, schemaType.getName());
                    }
                    return new TransactionBufferSnapshotReader<>(reader, this);
                });
    }

}
