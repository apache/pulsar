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
package org.apache.pulsar.client.impl.transaction;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * The default implementation of {@link Transaction}.
 *
 * <p>All the error handling and retry logic are handled by this class.
 * The original pulsar client doesn't handle any transaction logic. It is only responsible
 * for sending the messages and acknowledgements carrying the transaction id and retrying on
 * failures. This decouples the transactional operations from non-transactional operations as
 * much as possible.
 */
@Getter
public class TransactionImpl implements Transaction {

    @Data
    private static class TransactionalSendOp {
        private final CompletableFuture<MessageId> sendFuture;
        private final CompletableFuture<MessageId> transactionalSendFuture;
    }

    @Data
    private static class TransactionalAckOp {
        private final CompletableFuture<Void> ackFuture;
        private final CompletableFuture<Void> transactionalAckFuture;
    }

    private final PulsarClientImpl client;
    private final long transactionTimeoutMs;
    private final long txnIdLeastBits;
    private final long txnIdMostBits;
    private final AtomicLong sequenceId = new AtomicLong(0L);
    private final LinkedHashMap<Long, TransactionalSendOp> sendOps;
    private final Set<String> producedTopics;
    private final Set<TransactionalAckOp> ackOps;
    private final Set<String> ackedTopics;

    TransactionImpl(PulsarClientImpl client,
                    long transactionTimeoutMs,
                    long txnIdLeastBits,
                    long txnIdMostBits) {
        this.client = client;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.txnIdLeastBits = txnIdLeastBits;
        this.txnIdMostBits = txnIdMostBits;
        this.sendOps = new LinkedHashMap<>();
        this.producedTopics = new HashSet<>();
        this.ackOps = new HashSet<>();
        this.ackedTopics = new HashSet<>();
    }

    public long nextSequenceId() {
        return sequenceId.getAndIncrement();
    }

    // register the topics that will be modified by this transaction
    public synchronized void registerProducedTopic(String topic) {
        if (producedTopics.add(topic)) {
            // TODO: we need to issue the request to TC to register the produced topic
        }
    }

    public synchronized CompletableFuture<MessageId> registerSendOp(long sequenceId,
                                                                    CompletableFuture<MessageId> sendFuture) {
        CompletableFuture<MessageId> transactionalSendFuture = new CompletableFuture<>();
        TransactionalSendOp sendOp = new TransactionalSendOp(
            sendFuture,
            transactionalSendFuture
        );
        sendOps.put(sequenceId, sendOp);
        return transactionalSendFuture;
    }

    // register the topics that will be modified by this transaction
    public synchronized void registerAckedTopic(String topic) {
        if (ackedTopics.add(topic)) {
            // TODO: we need to issue the request to TC to register the acked topic
        }
    }

    public synchronized CompletableFuture<Void> registerAckOp(CompletableFuture<Void> ackFuture) {
        CompletableFuture<Void> transactionalAckFuture = new CompletableFuture<>();
        TransactionalAckOp ackOp = new TransactionalAckOp(
            ackFuture,
            transactionalAckFuture
        );
        ackOps.add(ackOp);
        return transactionalAckFuture;
    }

    @Override
    public CompletableFuture<Void> commit() {
        return FutureUtil.failedFuture(new UnsupportedOperationException("Not Implemented Yet"));
    }

    @Override
    public CompletableFuture<Void> abort() {
        return FutureUtil.failedFuture(new UnsupportedOperationException("Not Implemented Yet"));
    }
}
