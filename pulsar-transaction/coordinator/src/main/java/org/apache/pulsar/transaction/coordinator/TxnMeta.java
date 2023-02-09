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
package org.apache.pulsar.transaction.coordinator;

import com.google.common.annotations.Beta;
import java.util.List;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;

/**
 * An interface represents the metadata of a transaction in {@link TransactionMetadataStore}.
 */
@Beta
public interface TxnMeta {

    /**
     * Return the transaction id.
     *
     * @return transaction id.
     */
    TxnID id();

    /**
     * Return the transaction status.
     *
     * @return transaction status.
     */
    TxnStatus status();

    /**
     * Return the the list of partitions that this transaction produces to.
     *
     * @return the list of partitions that this transaction produced to.
     *         the returned list is sorted by partition name.
     */
    List<String> producedPartitions();

    /**
     * Return the the list of partitions that this transaction acknowledges to.
     *
     * @return the list of partitions that this transaction acknowledges to.
     *         the returned list is sorted by partition name.
     */
    List<TransactionSubscription> ackedPartitions();

    /**
     * Add the list of produced partitions to the transaction.
     *
     * @return transaction meta
     * @throws InvalidTxnStatusException if the transaction is not in
     *         {@link TxnStatus#OPEN}
     */
    TxnMeta addProducedPartitions(List<String> partitions)
        throws InvalidTxnStatusException;

    /**
     * Add the list of acked partitions to the transaction.
     *
     * @param subscriptions the ackd subscriptions add to the transaction
     * @return transaction meta
     * @throws InvalidTxnStatusException if the transaction is not in
     *         {@link TxnStatus#OPEN}
     */
    TxnMeta addAckedPartitions(List<TransactionSubscription> subscriptions)
        throws InvalidTxnStatusException;

    /**
     * Update the transaction stats from the <tt>newStatus</tt> only when
     * the current status is the expected <tt>expectedStatus</tt>.
     *
     * @param newStatus the new transaction status
     * @param expectedStatus the expected transaction status
     * @return the transaction itself.
     * @throws InvalidTxnStatusException if the transaction is not in the expected
     *         status, or it can not be transitioned to the new status
     */
    TxnMeta updateTxnStatus(TxnStatus newStatus,
                            TxnStatus expectedStatus) throws InvalidTxnStatusException;

    /**
     * Return the transaction open timestamp.
     *
     * @return transaction open timestamp.
     */
    long getOpenTimestamp();

    /**
     * Return the transaction timeout at.
     *
     * @return transaction timeout at.
     */
    long getTimeoutAt();

    /**
     * Return the transaction's owner.
     *
     * @return transaction's owner.
     */
    String getOwner();
}
