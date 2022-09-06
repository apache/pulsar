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
import org.apache.pulsar.broker.service.TransactionBufferSnapshotIndexService;
import org.apache.pulsar.broker.transaction.buffer.matadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class TransactionBufferSnapshotIndexSystemTopicClient extends
        SystemTopicClientBase<TransactionBufferSnapshotIndexes> {

    private final TransactionBufferSnapshotIndexService transactionBufferSnapshotIndexService;

    public TransactionBufferSnapshotIndexSystemTopicClient(PulsarClient client, TopicName topicName,
                                                           TransactionBufferSnapshotIndexService
                                                                     transactionBufferSnapshotIndexService) {
        super(client, topicName);
        this.transactionBufferSnapshotIndexService = transactionBufferSnapshotIndexService;
    }

    @Override
    protected CompletableFuture<Writer<TransactionBufferSnapshotIndexes>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(TransactionBufferSnapshotIndexes.class))
                .topic(topicName.toString())
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new transactionBufferSnapshot writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new TransactionBufferSnapshotIndexWriter(producer, this));
                });
    }

    @Override
    protected CompletableFuture<Reader<TransactionBufferSnapshotIndexes>> newReaderAsyncInternal() {
        return client.newReader(Schema.AVRO(TransactionBufferSnapshotIndexes.class))
                .topic(topicName.toString())
                .startMessageId(MessageId.earliest)
                .readCompacted(true)
                .createAsync()
                .thenCompose(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new transactionBufferSnapshot buffer reader is created", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new TransactionBufferSnapshotIndexReader(reader, this));
                });
    }

    protected void removeWriter(TransactionBufferSnapshotIndexWriter writer) {
        writers.remove(writer);
        this.transactionBufferSnapshotIndexService.removeClient(topicName, this);
    }

    protected void removeReader(TransactionBufferSnapshotIndexReader reader) {
        readers.remove(reader);
        this.transactionBufferSnapshotIndexService.removeClient(topicName, this);
    }

    private static class TransactionBufferSnapshotIndexWriter implements Writer<TransactionBufferSnapshotIndexes> {

        private final Producer<TransactionBufferSnapshotIndexes> producer;
        private final TransactionBufferSnapshotIndexSystemTopicClient
                transactionBufferSnapshotIndexSystemTopicClient;

        private TransactionBufferSnapshotIndexWriter(Producer<TransactionBufferSnapshotIndexes> producer,
                                                     TransactionBufferSnapshotIndexSystemTopicClient
                                                             transactionBufferSnapshotIndexSystemTopicClient) {
            this.producer = producer;
            this.transactionBufferSnapshotIndexSystemTopicClient = transactionBufferSnapshotIndexSystemTopicClient;
        }

        @Override
        public MessageId write(TransactionBufferSnapshotIndexes transactionBufferSnapshotIndexes)
                throws PulsarClientException {
            return producer.newMessage().key(transactionBufferSnapshotIndexes.getTopicName())
                    .value(transactionBufferSnapshotIndexes).send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(TransactionBufferSnapshotIndexes
                                                                       transactionBufferSnapshotIndexes) {
            return producer.newMessage()
                    .key(transactionBufferSnapshotIndexes.getTopicName())
                    .value(transactionBufferSnapshotIndexes).sendAsync();
        }

        @Override
        public MessageId delete(TransactionBufferSnapshotIndexes transactionBufferSnapshotIndexes)
                throws PulsarClientException {
            return producer.newMessage()
                    .key(transactionBufferSnapshotIndexes.getTopicName())
                    .value(null)
                    .send();
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(TransactionBufferSnapshotIndexes
                                                                        transactionBufferSnapshotIndexes) {
            return producer.newMessage()
                    .key(transactionBufferSnapshotIndexes.getTopicName())
                    .value(null)
                    .sendAsync();
        }

        @Override
        public void close() throws IOException {
            this.producer.close();
            transactionBufferSnapshotIndexSystemTopicClient.removeWriter(this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            producer.closeAsync().whenComplete((v, e) -> {
                // if close fail, also need remove the producer
                transactionBufferSnapshotIndexSystemTopicClient.removeWriter(this);
                if (e != null) {
                    completableFuture.completeExceptionally(e);
                    return;
                }
                completableFuture.complete(null);
            });
            return completableFuture;
        }

        @Override
        public SystemTopicClient<TransactionBufferSnapshotIndexes> getSystemTopicClient() {
            return transactionBufferSnapshotIndexSystemTopicClient;
        }
    }

    private static class TransactionBufferSnapshotIndexReader implements Reader<TransactionBufferSnapshotIndexes> {

        private final org.apache.pulsar.client.api.Reader<TransactionBufferSnapshotIndexes> reader;
        private final TransactionBufferSnapshotIndexSystemTopicClient
                transactionBufferSnapshotIndexSystemTopicClient;

        private TransactionBufferSnapshotIndexReader(
                org.apache.pulsar.client.api.Reader<TransactionBufferSnapshotIndexes> reader,
                TransactionBufferSnapshotIndexSystemTopicClient transactionBufferSnapshotIndexSystemTopicClient) {
            this.reader = reader;
            this.transactionBufferSnapshotIndexSystemTopicClient = transactionBufferSnapshotIndexSystemTopicClient;
        }

        @Override
        public Message<TransactionBufferSnapshotIndexes> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<TransactionBufferSnapshotIndexes>> readNextAsync() {
            return reader.readNextAsync();
        }

        @Override
        public Message<TransactionBufferSnapshotIndexes> readByMessageId(MessageId messageId)
                throws PulsarClientException {
            MessageIdImpl messageIdImpl = (MessageIdImpl) messageId;
            reader.seek(new MessageIdImpl(messageIdImpl.getLedgerId(), messageIdImpl.getEntryId() - 1,
                    messageIdImpl.getPartitionIndex()));
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<TransactionBufferSnapshotIndexes>> readByMessageIdAsync(MessageId messageId) {
            MessageIdImpl messageIdImpl = (MessageIdImpl) messageId;
            return reader.seekAsync(new MessageIdImpl(messageIdImpl.getLedgerId(), messageIdImpl.getEntryId() - 1,
                    messageIdImpl.getPartitionIndex())).thenCompose((ignore) -> {
                return reader.readNextAsync();
            });
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
            transactionBufferSnapshotIndexSystemTopicClient.removeReader(this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            reader.closeAsync().whenComplete((v, e) -> {
                // if close fail, also need remove the reader
                transactionBufferSnapshotIndexSystemTopicClient.removeReader(this);
                if (e != null) {
                    completableFuture.completeExceptionally(e);
                    return;
                }
                completableFuture.complete(null);
            });
            return completableFuture;
        }

        @Override
        public SystemTopicClient<TransactionBufferSnapshotIndexes> getSystemTopic() {
            return transactionBufferSnapshotIndexSystemTopicClient;
        }
    }
}

