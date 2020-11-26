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
package org.apache.pulsar.broker.transaction.buffer.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;

import java.util.concurrent.CompletableFuture;

/**
 * A provider that provides topic implementations of {@link TransactionBuffer}.
 */
@Slf4j
public class TopicTransactionBufferProvider implements TransactionBufferProvider {

    @Override
    public CompletableFuture<TransactionBuffer> newTransactionBuffer() {
        CompletableFuture<TransactionBuffer> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception("Unsupported operation new transaction buffer "
                + "with no arguments for TopicTransactionBufferProvider."));
        return completableFuture;
    }

    @Override
    public CompletableFuture<TransactionBuffer> newTransactionBuffer(Topic originTopic) {
        CompletableFuture<TransactionBuffer> completableFuture = new CompletableFuture<>();
        try {
            completableFuture.complete(new TopicTransactionBuffer((PersistentTopic) originTopic));
        } catch (ManagedLedgerException e) {
            log.error("Topic : [{}] open transaction buffer fail!", originTopic.getName());
            completableFuture.completeExceptionally(e);
        }
        return completableFuture;
    }
}
