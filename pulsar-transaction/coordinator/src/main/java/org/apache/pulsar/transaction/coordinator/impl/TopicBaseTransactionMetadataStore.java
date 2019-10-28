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
package org.apache.pulsar.transaction.coordinator.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;

/**
 * The provider that offers in-memory implementation of {@link TransactionMetadataStore}.
 */
public class TopicBaseTransactionMetadataStore implements TransactionMetadataStore {

    private final TransactionCoordinatorID tcID;
    private final AtomicLong sequenceId;
    private final TopicBaseTransactionMetadataReader reader;
    private final TopicBaseTransactionMetadataWriter writer;

    TopicBaseTransactionMetadataStore(TransactionCoordinatorID tcID,
                                      TopicBaseTransactionMetadataReader reader,
                                      TopicBaseTransactionMetadataWriter writer) {
        this.tcID = tcID;
        this.sequenceId = new AtomicLong(reader.readSequenceId());
        this.reader = reader;
        this.writer = writer;

    }

    @Override
    public CompletableFuture<TxnStatus> getTxnStatus(TxnID txnid) {
        return null;
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnid) {

        return reader.read(txnid);
    }

    @Override
    public CompletableFuture<TxnID> newTransaction() {

        TxnID txnID = new TxnID(
                tcID.getId(),
                sequenceId.getAndIncrement()
        );
        return writer.write(new TxnMetaImpl(txnID))
                .thenCompose((txn) -> {
                    reader.flushCache(txn);
                    return CompletableFuture.completedFuture(txnID);
                });
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnid, List<String> partitions) {
        return getTxnMeta(txnid).thenCompose(tnx -> {
            try {
                return writer.write(tnx.addProducedPartitions(partitions))
                        .thenCompose(txnMeta -> reader.flushCache(txnMeta));
            } catch (InvalidTxnStatusException e) {
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                completableFuture.completeExceptionally(e);
                return completableFuture;
            }
        });
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnid, List<String> partitions) {
        return getTxnMeta(txnid).thenCompose(tnx -> {
            try {
                return writer.write(tnx.addAckedPartitions(partitions))
                        .thenCompose(txnMeta -> reader.flushCache(txnMeta));
            } catch (InvalidTxnStatusException e) {
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                completableFuture.completeExceptionally(e);
                return completableFuture;
            }
        });
    }

    @Override
    public CompletableFuture<Void> updateTxnStatus(TxnID txnid, TxnStatus newStatus, TxnStatus expectedStatus) {
        return getTxnMeta(txnid).thenCompose(tnx -> {
            try {
                return writer.write(tnx.updateTxnStatus(newStatus, expectedStatus))
                        .thenCompose(txnMeta -> reader.flushCache(txnMeta));
            } catch (InvalidTxnStatusException e) {
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                completableFuture.completeExceptionally(e);
                return completableFuture;
            }
        });
    }

    public interface TopicBaseTransactionMetadataReader {

        /**
         * Query the {@link TxnMeta} of a given transaction <tt>txnid</tt>.
         *
         * @param txnid transaction id
         * @return a future represents the result of this operation.
         *         it returns {@link TxnMeta} of the given transaction.
         */
        CompletableFuture<TxnMeta> read(TxnID txnid);

        /**
         * Query the sequenceId for new {@link TxnID}
         *
         * @return {@link Long} for generate sequenceId
         */
        Long readSequenceId();

        CompletableFuture<Void> flushCache(TxnMeta txnMeta);
    }

    public interface TopicBaseTransactionMetadataWriter {

        /**
         * Update the transaction from <tt>expectedStatus</tt> to <tt>newStatus</tt>.
         *
         * <p>If the current transaction status is not <tt>expectedStatus</tt>, the
         * update will be failed.
         * @return a future represents the result of the operation
         */
        CompletableFuture<TxnMeta> write(TxnMeta txnMeta);
    }
}
