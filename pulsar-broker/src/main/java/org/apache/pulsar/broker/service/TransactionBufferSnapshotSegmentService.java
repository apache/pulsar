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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.TransactionBufferSnapshotSegmentSystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.matadata.v2.TransactionBufferSnapshot;
import org.apache.pulsar.common.naming.TopicName;

public interface TransactionBufferSnapshotSegmentService {
    /**
     * Create a transaction buffer snapshot segment writer.
     *
     * @param topicName {@link TopicName} the topic name
     *
     * @return {@link CompletableFuture< SystemTopicClient.Writer >} return the future of writer
     */
    CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshot>> createWriter(TopicName topicName);

    /**
     * Create a transaction buffer snapshot segment reader.
     *
     * @param topicName {@link TopicName} the topic name
     *
     * @return {@link CompletableFuture< SystemTopicClient.Reader >} return the future of reader
     */
    CompletableFuture<SystemTopicClient.Reader<TransactionBufferSnapshot>> createReader(TopicName topicName);

    /**
     * Remove a topic client from cache.
     *
     * @param topicName {@link TopicName} the topic name
     * @param transactionBufferSnapshotSegmentSystemTopicClient
     * {@link TransactionBufferSnapshotSegmentSystemTopicClient} the topic client
     *
     */
    void removeClient(TopicName topicName, TransactionBufferSnapshotSegmentSystemTopicClient
            transactionBufferSnapshotSegmentSystemTopicClient);

    /**
     * Close transaction buffer snapshot service.
     */
    void close() throws Exception;
}
