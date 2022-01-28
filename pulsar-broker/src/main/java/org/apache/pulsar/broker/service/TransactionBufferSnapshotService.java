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
import org.apache.pulsar.broker.systopic.SystemTopicClient.Reader;
import org.apache.pulsar.broker.systopic.SystemTopicClient.Writer;
import org.apache.pulsar.broker.systopic.TransactionBufferSystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.common.naming.TopicName;

public interface TransactionBufferSnapshotService {

    /**
     * Create a transaction buffer snapshot writer.
     *
     * @param topicName {@link TopicName} the topic name
     *
     * @return {@link CompletableFuture<Writer>} return the future of writer
     */
    CompletableFuture<Writer<TransactionBufferSnapshot>> createWriter(TopicName topicName);

    /**
     * Create a transaction buffer snapshot reader.
     *
     * @param topicName {@link TopicName} the topic name
     *
     * @return {@link CompletableFuture<Writer>} return the future of reader
     */
    CompletableFuture<Reader<TransactionBufferSnapshot>> createReader(TopicName topicName);

    /**
     * Remove a topic client from cache.
     *
     * @param topicName {@link TopicName} the topic name
     * @param transactionBufferSystemTopicClient {@link TransactionBufferSystemTopicClient} the topic client
     *
     */
    void removeClient(TopicName topicName, TransactionBufferSystemTopicClient transactionBufferSystemTopicClient);

    /**
     * Close transaction buffer snapshot service.
     */
    void close() throws Exception;

}
