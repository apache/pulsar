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

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotService;
import org.apache.pulsar.broker.transaction.buffer.matadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class TransactionBufferSnapshotSegmentSystemTopicClient extends
        TransactionBufferSnapshotBaseSystemTopicClient<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot> {

    public <T> TransactionBufferSnapshotSegmentSystemTopicClient(PulsarClient client, TopicName topicName,
              TransactionBufferSnapshotService<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot>
                                                                     transactionBufferSnapshotSegmentService) {
        super(client, topicName, transactionBufferSnapshotSegmentService);
    }

    @Override
    protected CompletableFuture<Writer<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot>>
    newWriterAsyncInternal() {
        return client.newProducer(Schema.AVRO(TransactionBufferSnapshotIndexes.TransactionBufferSnapshot.class))
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
    protected CompletableFuture<Reader<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot>>
    newReaderAsyncInternal() {
        return client.newReader(Schema.AVRO(TransactionBufferSnapshotIndexes.TransactionBufferSnapshot.class))
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

    protected static String buildKey(TransactionBufferSnapshotIndexes.TransactionBufferSnapshot snapshot) {
        return "multiple-" + snapshot.getSequenceId() + "-" + snapshot.getTopicName();
    }


    private static class TransactionBufferSnapshotSegmentWriter extends TransactionBufferSnapshotBaseSystemTopicClient
            .TransactionBufferSnapshotBaseWriter<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot> {

        private TransactionBufferSnapshotSegmentWriter(Producer<TransactionBufferSnapshotIndexes
                .TransactionBufferSnapshot> producer, TransactionBufferSnapshotSegmentSystemTopicClient
                                                             transactionBufferSnapshotSegmentSystemTopicClient) {
            super(producer, transactionBufferSnapshotSegmentSystemTopicClient);
        }

        @Override
        public MessageId write(TransactionBufferSnapshotIndexes.TransactionBufferSnapshot transactionBufferSnapshot)
                throws PulsarClientException {
            return producer.newMessage()
                    .key(buildKey(transactionBufferSnapshot))
                    .value(transactionBufferSnapshot).send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(TransactionBufferSnapshotIndexes.TransactionBufferSnapshot
                                                                       transactionBufferSnapshot) {
            return producer.newMessage()
                    .key(buildKey(transactionBufferSnapshot))
                    .value(transactionBufferSnapshot).sendAsync();
        }

        @Override
        public MessageId delete(TransactionBufferSnapshotIndexes.TransactionBufferSnapshot transactionBufferSnapshot)
                throws PulsarClientException {
            return producer.newMessage()
                    .key(buildKey(transactionBufferSnapshot))
                    .value(null)
                    .send();
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(TransactionBufferSnapshotIndexes.TransactionBufferSnapshot
                                                                transactionBufferSnapshot) {
            return producer.newMessage()
                    .key(buildKey(transactionBufferSnapshot))
                    .value(null)
                    .sendAsync();
        }
    }

    private static class TransactionBufferSnapshotSegmentReader extends TransactionBufferSnapshotBaseSystemTopicClient
            .TransactionBufferSnapshotBaseReader<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot> {

        private TransactionBufferSnapshotSegmentReader(
                org.apache.pulsar.client.api.Reader<TransactionBufferSnapshotIndexes.TransactionBufferSnapshot> reader,
                TransactionBufferSnapshotSegmentSystemTopicClient transactionBufferSnapshotSegmentSystemTopicClient) {
            super(reader, transactionBufferSnapshotSegmentSystemTopicClient);
        }
    }
}
