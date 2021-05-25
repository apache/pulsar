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
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStatus;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionMetadata;

public interface Transactions {

    /**
     * Get transaction metadataStore status.
     *
     * @param coordinatorId the id which get transaction coordinator
     * @return the list future of transaction metadata store status.
     */
    CompletableFuture<TransactionCoordinatorStatus> getCoordinatorStatusById(int coordinatorId);

    /**
     * Get transaction metadataStore status.
     *
     * @return the map future of transaction metadata store status.
     */
    CompletableFuture<Map<Integer, TransactionCoordinatorStatus>> getCoordinatorStatus();

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
     * @param subName the sub name of this transaction ack
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
     * Get slow transaction metadata by coordinatorId.
     *
     * @param coordinatorId the coordinator id of getting slow transaction status.
     * @param timeout the timeout
     * @param timeUnit the timeout timeUnit
     * @return the future metadata of slow transactions.
     */
    CompletableFuture<Map<String, TransactionMetadata>> getSlowTransactionMetadataByCoordinatorId(Integer coordinatorId,
                                                                                                  long timeout,
                                                                                                  TimeUnit timeUnit);

    /**
     * Get slow transaction metadata.
     *
     * @param timeout the timeout
     * @param timeUnit the timeout timeUnit
     *
     * @return the future metadata of slow transactions.
     */
    CompletableFuture<Map<String, TransactionMetadata>> getSlowTransactionMetadata(long timeout, TimeUnit timeUnit);

}
