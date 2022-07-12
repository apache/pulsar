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
package org.apache.pulsar.client.admin;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorInternalStats;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionMetadata;
import org.apache.pulsar.common.policies.data.TransactionPendingAckInternalStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.common.stats.PositionInPendingAckStats;

public interface Transactions {

    /**
     * Get transaction metadataStore stats.
     *
     * @param coordinatorId the id which get transaction coordinator
     * @return the future of transaction metadata store stats.
     */
    CompletableFuture<TransactionCoordinatorStats> getCoordinatorStatsByIdAsync(int coordinatorId);

    /**
     * Get transaction metadataStore stats.
     *
     * @param coordinatorId the id which get transaction coordinator
     * @return the transaction metadata store stats.
     */
    TransactionCoordinatorStats getCoordinatorStatsById(int coordinatorId) throws PulsarAdminException;

    /**
     * Get transaction metadataStore stats.
     *
     * @return the map future of transaction metadata store stats.
     */
    CompletableFuture<Map<Integer, TransactionCoordinatorStats>> getCoordinatorStatsAsync();

    /**
     * Get transaction metadataStore stats.
     *
     * @return the map of transaction metadata store stats.
     */
    Map<Integer, TransactionCoordinatorStats> getCoordinatorStats() throws PulsarAdminException;

    /**
     * Get transaction in buffer stats.
     *
     * @param txnID the txnId
     * @param topic the produce topic
     * @return the future stats of transaction in buffer.
     */
    CompletableFuture<TransactionInBufferStats> getTransactionInBufferStatsAsync(TxnID txnID, String topic);

    /**
     * Get transaction in buffer stats.
     *
     * @param txnID the txnId
     * @param topic the produce topic
     * @return the stats of transaction in buffer.
     */
    TransactionInBufferStats getTransactionInBufferStats(TxnID txnID, String topic) throws PulsarAdminException;

    /**
     * Get transaction in pending ack stats.
     *
     * @param txnID the txnId
     * @param topic the ack topic
     * @param subName the subscription name of this transaction ack
     * @return the future stats of transaction in pending ack.
     */
    CompletableFuture<TransactionInPendingAckStats> getTransactionInPendingAckStatsAsync(TxnID txnID, String topic,
                                                                                         String subName);
    /**
     * Get transaction in pending ack stats.
     *
     * @param txnID the txnId
     * @param topic the ack topic
     * @param subName the subscription name of this transaction ack
     * @return the stats of transaction in pending ack.
     */
    TransactionInPendingAckStats getTransactionInPendingAckStats(TxnID txnID, String topic,
                                                                 String subName) throws PulsarAdminException;
    /**
     * Get transaction metadata.
     *
     * @param txnID the ID of this transaction
     * @return the future metadata of this transaction.
     */
    CompletableFuture<TransactionMetadata> getTransactionMetadataAsync(TxnID txnID);

    /**
     * Get transaction metadata.
     *
     * @param txnID the ID of this transaction
     * @return the metadata of this transaction.
     */
    TransactionMetadata getTransactionMetadata(TxnID txnID) throws PulsarAdminException;

    /**
     * Get transaction buffer stats.
     *
     * @param topic the topic of getting transaction buffer stats
     * @param  lowWaterMarks Whether to get information about lowWaterMarks stored in transaction pending ack.
     * @return the future stats of transaction buffer in topic.
     */
    CompletableFuture<TransactionBufferStats> getTransactionBufferStatsAsync(String topic, boolean lowWaterMarks);

    /**
     * Get transaction buffer stats.
     *
     * @param topic the topic of getting transaction buffer stats
     * @return the future stats of transaction buffer in topic.
     */
    default CompletableFuture<TransactionBufferStats> getTransactionBufferStatsAsync(String topic) {
        return getTransactionBufferStatsAsync(topic, false);
    }

    /**
     * Get transaction buffer stats.
     *
     * @param topic the topic of getting transaction buffer stats
     * @param  lowWaterMarks Whether to get information about lowWaterMarks stored in transaction buffer.
     * @return the stats of transaction buffer in topic.
     */
    TransactionBufferStats getTransactionBufferStats(String topic, boolean lowWaterMarks) throws PulsarAdminException;

    /**
     * Get transaction buffer stats.
     *
     * @param topic the topic of getting transaction buffer stats
     * @return the stats of transaction buffer in topic.
     */
    default TransactionBufferStats getTransactionBufferStats(String topic) throws PulsarAdminException {
        return getTransactionBufferStats(topic, false);
    }

    /**
     * Get transaction pending ack stats.
     *
     * @param topic the topic of this transaction pending ack stats
     * @param subName the subscription name of this transaction pending ack stats
     * @param  lowWaterMarks Whether to get information about lowWaterMarks stored in transaction pending ack.
     * @return the stats of transaction pending ack.
     */
    CompletableFuture<TransactionPendingAckStats> getPendingAckStatsAsync(String topic, String subName,
                                                                          boolean lowWaterMarks);

    /**
     * Get transaction pending ack stats.
     *
     * @param topic the topic of this transaction pending ack stats
     * @param subName the subscription name of this transaction pending ack stats
     * @return the stats of transaction pending ack.
     */
    default CompletableFuture<TransactionPendingAckStats> getPendingAckStatsAsync(String topic, String subName) {
        return getPendingAckStatsAsync(topic, subName, false);
    }

    /**
     * Get transaction pending ack stats.
     *
     * @param topic the topic of this transaction pending ack stats
     * @param subName the subscription name of this transaction pending ack stats
     * @param  lowWaterMarks Whether to get information about lowWaterMarks stored in transaction pending ack.
     * @return the stats of transaction pending ack.
     */
    TransactionPendingAckStats getPendingAckStats(String topic, String subName, boolean lowWaterMarks)
            throws PulsarAdminException;

    /**
     * Get transaction pending ack stats.
     *
     * @param topic the topic of this transaction pending ack stats
     * @param subName the subscription name of this transaction pending ack stats
     * @return the stats of transaction pending ack.
     */
    default TransactionPendingAckStats getPendingAckStats(String topic, String subName) throws PulsarAdminException {
        return getPendingAckStats(topic, subName, false);
    }

    /**
     * Get slow transactions by coordinator id.
     *
     * @param coordinatorId the coordinator id of getting slow transaction status.
     * @param timeout the timeout
     * @param timeUnit the timeout timeUnit
     * @return the future metadata of slow transactions.
     */
    CompletableFuture<Map<String, TransactionMetadata>> getSlowTransactionsByCoordinatorIdAsync(Integer coordinatorId,
                                                                                                long timeout,
                                                                                                TimeUnit timeUnit);

    /**
     * Get slow transactions by coordinator id.
     *
     * @param coordinatorId the coordinator id of getting slow transaction status.
     * @param timeout the timeout
     * @param timeUnit the timeout timeUnit
     * @return the metadata of slow transactions.
     */
    Map<String, TransactionMetadata> getSlowTransactionsByCoordinatorId(Integer coordinatorId,
                                                                        long timeout,
                                                                        TimeUnit timeUnit) throws PulsarAdminException;

    /**
     * Get slow transactions.
     *
     * @param timeout the timeout
     * @param timeUnit the timeout timeUnit
     *
     * @return the future metadata of slow transactions.
     */
    CompletableFuture<Map<String, TransactionMetadata>> getSlowTransactionsAsync(long timeout,
                                                                                 TimeUnit timeUnit);


    /**
     * Get slow transactions.
     *
     * @param timeout the timeout
     * @param timeUnit the timeout timeUnit
     *
     * @return the metadata of slow transactions.
     */
    Map<String, TransactionMetadata> getSlowTransactions(long timeout, TimeUnit timeUnit) throws PulsarAdminException;

    /**
     * Get transaction coordinator internal stats.
     *
     * @param coordinatorId the coordinator ID
     * @param metadata is get ledger metadata
     *
     * @return the future internal stats of this coordinator
     */
    CompletableFuture<TransactionCoordinatorInternalStats> getCoordinatorInternalStatsAsync(int coordinatorId,
                                                                                            boolean metadata);

    /**
     * Get transaction coordinator internal stats.
     *
     * @param coordinatorId the coordinator ID
     * @param metadata whether to obtain ledger metadata
     *
     * @return the internal stats of this coordinator
     */
    TransactionCoordinatorInternalStats getCoordinatorInternalStats(int coordinatorId,
                                                                    boolean metadata) throws PulsarAdminException;

    /**
     * Get pending ack internal stats.
     *
     * @param topic the topic of get pending ack internal stats
     * @param subName the subscription name of this pending ack
     * @param metadata whether to obtain ledger metadata
     *
     * @return the future internal stats of pending ack
     */
    CompletableFuture<TransactionPendingAckInternalStats> getPendingAckInternalStatsAsync(String topic, String subName,
                                                                                          boolean metadata);

    /**
     * Get pending ack internal stats.
     *
     * @param topic the topic of get pending ack internal stats
     * @param subName the subscription name of this pending ack
     * @param metadata whether to obtain ledger metadata
     *
     * @return the internal stats of pending ack
     */
    TransactionPendingAckInternalStats getPendingAckInternalStats(String topic, String subName,
                                                                  boolean metadata) throws PulsarAdminException;

    /**
     * Sets the scale of the transaction coordinators.
     * And currently, we can only support scale-up.
     * @param replicas the new transaction coordinators size.
     */
    void scaleTransactionCoordinators(int replicas) throws PulsarAdminException;

    /**
     * Asynchronously sets the size of the transaction coordinators.
     * And currently, we can only support scale-up.
     * @param replicas the new transaction coordinators size.
     * @return a future that can be used to track when the transaction coordinator number is updated.
     */
    CompletableFuture<Void> scaleTransactionCoordinatorsAsync(int replicas);

    /**
     * Get the position stats in transaction pending ack.
     * @param topic the topic of checking position in pending ack state
     * @param subName the subscription name of this pending ack
     * @param ledgerId the ledger id of the message position.
     * @param entryId the entry id of the message position.
     * @param batchIndex the batch index of the message position, `null` means not batch message.
     * @return {@link PositionInPendingAckStats} a state identified whether the position state.
     */
    PositionInPendingAckStats getPositionStatsInPendingAck(String topic, String subName, Long ledgerId, Long entryId,
                                                           Integer batchIndex) throws PulsarAdminException;

    /**
     * Get the position stats in transaction pending ack.
     *
     * @param topic the topic of checking position in pending ack state
     * @param subName the subscription name of this pending ack
     * @param ledgerId the ledger id of the message position.
     * @param entryId the entry id of the message position.
     * @param batchIndex the batch index of the message position, `null` means not batch message.
     * @return {@link PositionInPendingAckStats} a state identified whether the position state.
     */
    CompletableFuture<PositionInPendingAckStats> getPositionStatsInPendingAckAsync(String topic, String subName,
                                                                                   Long ledgerId, Long entryId,
                                                                                   Integer batchIndex);
}
