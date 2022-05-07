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
package org.apache.pulsar.client.api.transaction;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * The transaction buffer client to commit and abort transactions on topics or subscription.
 * The transaction buffer client is used by transaction coordinator to end transactions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TransactionBufferClient {

    /**
     * Commit the transaction associated with the topic.
     *
     * @param topic topic name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @param lowWaterMark the low water mark of this txn;
     * @return the future represents the commit result
     */
    CompletableFuture<TxnID> commitTxnOnTopic(String topic,
                                              long txnIdMostBits,
                                              long txnIdLeastBits,
                                              long lowWaterMark);

    /**
     * Abort the transaction associated with the topic.
     *
     * @param topic topic name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @param lowWaterMark the low water mark of this txn
     * @return the future represents the abort result
     */
    CompletableFuture<TxnID> abortTxnOnTopic(String topic,
                                             long txnIdMostBits,
                                             long txnIdLeastBits,
                                             long lowWaterMark);

    /**
     * Commit the transaction associated with the topic subscription.
     *
     * @param topic topic name
     * @param subscription subscription name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @param lowWaterMark the low water mark of this txn
     * @return the future represents the commit result
     */
    CompletableFuture<TxnID> commitTxnOnSubscription(String topic,
                                                     String subscription,
                                                     long txnIdMostBits,
                                                     long txnIdLeastBits,
                                                     long lowWaterMark);

    /**
     * Abort the transaction associated with the topic subscription.
     *
     * @param topic topic name
     * @param subscription subscription name
     * @param txnIdMostBits the most bits of txn id
     * @param txnIdLeastBits the least bits of txn id
     * @param lowWaterMark the low water mark of this txn
     * @return the future represents the abort result
     */
    CompletableFuture<TxnID> abortTxnOnSubscription(String topic,
                                                    String subscription,
                                                    long txnIdMostBits,
                                                    long txnIdLeastBits,
                                                    long lowWaterMark);

    void close();

    int getAvailableRequestCredits();

    int getPendingRequestsCount();
}
