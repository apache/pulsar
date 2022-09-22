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
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class TransactionBufferSnapshotSystemTopicClient extends
        TransactionBufferSnapshotBaseSystemTopicClient<TransactionBufferSnapshot> {

    public TransactionBufferSnapshotSystemTopicClient(PulsarClient client, TopicName topicName,
                                                      SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshot>
                                                              systemTopicTxnBufferSnapshotService) {
        super(client, topicName, systemTopicTxnBufferSnapshotService);
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
                            new TransactionBufferSnapshotBaseWriter(producer, this));
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
                            new TransactionBufferSnapshotBaseReader<>(reader, this));
                });
    }
}

