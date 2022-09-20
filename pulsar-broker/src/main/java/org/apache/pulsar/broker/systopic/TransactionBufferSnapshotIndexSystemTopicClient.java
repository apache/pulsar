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
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService;
import org.apache.pulsar.broker.transaction.buffer.matadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class TransactionBufferSnapshotIndexSystemTopicClient extends
        TransactionBufferSnapshotBaseSystemTopicClient<TransactionBufferSnapshotIndexes> {

    private final SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotIndexes>
            transactionBufferSnapshotIndexService;

    public TransactionBufferSnapshotIndexSystemTopicClient(PulsarClient client, TopicName topicName,
           SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotIndexes>
                   transactionBufferSnapshotIndexService) {
        super(client, topicName, transactionBufferSnapshotIndexService);
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

    private static class TransactionBufferSnapshotIndexWriter extends
            TransactionBufferSnapshotBaseWriter<TransactionBufferSnapshotIndexes> {


        private TransactionBufferSnapshotIndexWriter(Producer<TransactionBufferSnapshotIndexes> producer,
                                                     TransactionBufferSnapshotIndexSystemTopicClient
                                                             transactionBufferSnapshotIndexSystemTopicClient) {
            super(producer, transactionBufferSnapshotIndexSystemTopicClient);
        }
    }

    private static class TransactionBufferSnapshotIndexReader extends TransactionBufferSnapshotBaseSystemTopicClient
            .TransactionBufferSnapshotBaseReader<TransactionBufferSnapshotIndexes> {
        private TransactionBufferSnapshotIndexReader(
                org.apache.pulsar.client.api.Reader<TransactionBufferSnapshotIndexes> reader,
                TransactionBufferSnapshotIndexSystemTopicClient transactionBufferSnapshotIndexSystemTopicClient) {
            super(reader, transactionBufferSnapshotIndexSystemTopicClient);
        }
    }
}

