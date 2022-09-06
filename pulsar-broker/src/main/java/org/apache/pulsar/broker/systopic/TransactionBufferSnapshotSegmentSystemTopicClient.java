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
import org.apache.pulsar.broker.service.TransactionBufferSnapshotSegmentService;
import org.apache.pulsar.broker.transaction.buffer.matadata.v2.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class TransactionBufferSnapshotSegmentSystemTopicClient extends
        SystemTopicClientBase<TransactionBufferSnapshot> {

    private final TransactionBufferSnapshotSegmentService transactionBufferSnapshotSegmentService;
    public TransactionBufferSnapshotSegmentSystemTopicClient(PulsarClient client, TopicName topicName,
                                                             TransactionBufferSnapshotSegmentService
                                                                     transactionBufferSnapshotSegmentService) {
        super(client, topicName);
        this.transactionBufferSnapshotSegmentService = transactionBufferSnapshotSegmentService;
    }

    @Override
    protected CompletableFuture<Writer<TransactionBufferSnapshot>> newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(TransactionBufferSnapshot.class))
                .topic(topicName.toString())
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new transactionBufferSnapshot segment writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new TransactionBufferSnapshotSegmentWriter(producer, this));
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
                        log.debug("[{}] A new transactionBufferSnapshot buffer snapshot segment reader is created",
                                topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new TransactionBufferSnapshotSegmentReader(reader, this));
                });
    }

    protected static String buildKey(TransactionBufferSnapshot snapshot) {
        return "multiple-" + snapshot.getSequenceId() + "-" + snapshot.getTopicName();
    }

    protected void removeWriter(TransactionBufferSnapshotSegmentWriter writer) {
        writers.remove(writer);
        this.transactionBufferSnapshotSegmentService.removeClient(topicName, this);
    }

    protected void removeReader(TransactionBufferSnapshotSegmentReader reader) {
        readers.remove(reader);
        this.transactionBufferSnapshotSegmentService.removeClient(topicName, this);
    }


    private static class TransactionBufferSnapshotSegmentWriter implements Writer<TransactionBufferSnapshot> {

        private final Producer<TransactionBufferSnapshot> producer;
        private final TransactionBufferSnapshotSegmentSystemTopicClient
                transactionBufferSnapshotSegmentSystemTopicClient;

        private TransactionBufferSnapshotSegmentWriter(Producer<TransactionBufferSnapshot> producer,
                                                     TransactionBufferSnapshotSegmentSystemTopicClient
                                                             transactionBufferSnapshotSegmentSystemTopicClient) {
            this.producer = producer;
            this.transactionBufferSnapshotSegmentSystemTopicClient = transactionBufferSnapshotSegmentSystemTopicClient;
        }

        @Override
        public MessageId write(TransactionBufferSnapshot transactionBufferSnapshot)
                throws PulsarClientException {
            return producer.newMessage()
                    .key(buildKey(transactionBufferSnapshot))
                    .value(transactionBufferSnapshot).send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(TransactionBufferSnapshot
                                                                       transactionBufferSnapshot) {
            return producer.newMessage()
                    .key(buildKey(transactionBufferSnapshot))
                    .value(transactionBufferSnapshot).sendAsync();
        }

        @Override
        public MessageId delete(TransactionBufferSnapshot transactionBufferSnapshot)
                throws PulsarClientException {
            return producer.newMessage()
                    .key(buildKey(transactionBufferSnapshot))
                    .value(null)
                    .send();
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(TransactionBufferSnapshot
                                                                transactionBufferSnapshot) {
            return producer.newMessage()
                    .key(buildKey(transactionBufferSnapshot))
                    .value(null)
                    .sendAsync();
        }

        @Override
        public void close() throws IOException {
            this.producer.close();
            transactionBufferSnapshotSegmentSystemTopicClient.removeWriter(this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            producer.closeAsync().whenComplete((v, e) -> {
                // if close fail, also need remove the producer
                transactionBufferSnapshotSegmentSystemTopicClient.removeWriter(this);
                if (e != null) {
                    completableFuture.completeExceptionally(e);
                    return;
                }
                completableFuture.complete(null);
            });
            return completableFuture;
        }

        @Override
        public SystemTopicClient<TransactionBufferSnapshot> getSystemTopicClient() {
            return transactionBufferSnapshotSegmentSystemTopicClient;
        }
    }

    private static class TransactionBufferSnapshotSegmentReader implements Reader<TransactionBufferSnapshot> {

        private final org.apache.pulsar.client.api.Reader<TransactionBufferSnapshot> reader;
        private final TransactionBufferSnapshotSegmentSystemTopicClient
                transactionBufferSnapshotSegmentSystemTopicClient;

        private TransactionBufferSnapshotSegmentReader(
                org.apache.pulsar.client.api.Reader<TransactionBufferSnapshot> reader,
                TransactionBufferSnapshotSegmentSystemTopicClient transactionBufferSnapshotSegmentSystemTopicClient) {
            this.reader = reader;
            this.transactionBufferSnapshotSegmentSystemTopicClient = transactionBufferSnapshotSegmentSystemTopicClient;
        }

        @Override
        public Message<TransactionBufferSnapshot> readNext() throws PulsarClientException {
            throw new UnsupportedOperationException(
                    "Transaction buffer snapshot segment does not support sequential reads.");
        }

        @Override
        public CompletableFuture<Message<TransactionBufferSnapshot>> readNextAsync() {
            return FutureUtil.failedFuture(
                    new UnsupportedOperationException(
                            "Transaction buffer snapshot segment does not support sequential reads."));
        }

        @Override
        public Message<TransactionBufferSnapshot> readByMessageId(MessageId messageId) throws PulsarClientException {
            MessageIdImpl messageIdImpl = (MessageIdImpl) messageId;
            reader.seek(new MessageIdImpl(messageIdImpl.getLedgerId(), messageIdImpl.getEntryId() - 1,
                    messageIdImpl.getPartitionIndex()));
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<TransactionBufferSnapshot>> readByMessageIdAsync(MessageId messageId) {
            MessageIdImpl messageIdImpl = (MessageIdImpl) messageId;
            return reader.seekAsync(new MessageIdImpl(messageIdImpl.getLedgerId(),
                    messageIdImpl.getEntryId() - 1, messageIdImpl.getPartitionIndex()))
                    .thenCompose((ignore) -> reader.readNextAsync());
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
            transactionBufferSnapshotSegmentSystemTopicClient.removeReader(this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            reader.closeAsync().whenComplete((v, e) -> {
                // if close fail, also need remove the reader
                transactionBufferSnapshotSegmentSystemTopicClient.removeReader(this);
                if (e != null) {
                    completableFuture.completeExceptionally(e);
                    return;
                }
                completableFuture.complete(null);
            });
            return completableFuture;
        }

        @Override
        public SystemTopicClient<TransactionBufferSnapshot> getSystemTopic() {
            return transactionBufferSnapshotSegmentSystemTopicClient;
        }
    }
}
