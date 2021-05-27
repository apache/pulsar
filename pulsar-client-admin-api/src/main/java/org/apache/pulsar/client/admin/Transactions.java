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
import org.apache.pulsar.common.policies.data.CoordinatorInternalStats;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionMetadata;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;

public interface Transactions {

    /**
     * Get transaction metadataStore stats.
     *
     * @param coordinatorId the id which get transaction coordinator
     * @return the list future of transaction metadata store stats.
     */
    CompletableFuture<TransactionCoordinatorStats> getCoordinatorStatsById(int coordinatorId);

    /**
     * Get transaction metadataStore stats.
     *
     * @return the map future of transaction metadata store stats.
     */
    CompletableFuture<Map<Integer, TransactionCoordinatorStats>> getCoordinatorStats();

    /**
     * Get transaction in buffer stats.
     *
     * @param txnID the txnId
     * @param topic the produce topic
     * @return the future stats of transaction in buffer.
     */
    CompletableFuture<TransactionInBufferStats> getTransactionInBufferStats(TxnID txnID, String topic);

    /**
     * Get transaction in pending ack stats.
     *
     * @param txnID the txnId
     * @param topic the ack topic
     * @param subName the subscription name of this transaction ack
     * @return the future stats of transaction in pending ack.
     */
    CompletableFuture<TransactionInPendingAckStats> getTransactionInPendingAckStats(TxnID txnID, String topic,
                                                                                    String subName);

    /**
     * Get transaction metadata.
     *
     * @param txnID the ID of this transaction
     * @return the future metadata of this transaction.
     */
    CompletableFuture<TransactionMetadata> getTransactionMetadata(TxnID txnID);

    /**
     * Get transaction buffer stats.
     *
     * @param topic the topic of getting transaction buffer stats
     * @return the future stats of transaction buffer in topic.
     */
    CompletableFuture<TransactionBufferStats> getTransactionBufferStats(String topic);

    /**
     * Get transaction pending ack stats.
     *
     * @param topic the topic of this transaction pending ack stats
     * @param subName the subscription name of this transaction pending ack stats
     * @return the future stats of transaction pending ack.
     */
    CompletableFuture<TransactionPendingAckStats> getPendingAckStats(String topic, String subName);

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
     * @param coordinatorId the coordinator id
     * @param metadata is get ledger metadata
     *
     * @return the future internal stats of this coordinator
     */
    CompletableFuture<CoordinatorInternalStats> getCoordinatorInternalStatsAsync(int coordinatorId, boolean metadata);

    /**
     * Get transaction coordinator internal stats.
     *
     * @param coordinatorId the coordinator id
     * @param metadata is get ledger metadata
     *
     * @return the internal stats of this coordinator
     */
    CoordinatorInternalStats getCoordinatorInternalStats(int coordinatorId,
                                                         boolean metadata) throws PulsarAdminException;

}
