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

import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;

import java.util.concurrent.CompletableFuture;

/**
 * Interface of transaction buffer handler.
 */
public interface TransactionBufferHandler {

    /**
     * End transaction on topic.
     * @param topic topic name
     * @param txnIdMostBits txnIdMostBits
     * @param txnIdLeastBits txnIdLeastBits
     * @param action transaction action type
     * @return TxnId
     */
    CompletableFuture<TxnID> endTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits,
                                           PulsarApi.TxnAction action);

    /**
     * End transaction on subscription.
     * @param topic topic name
     * @param subscription subscription name
     * @param txnIdMostBits txnIdMostBits
     * @param txnIdLeastBits txnIdLeastBits
     * @param action transaction action type
     * @return TxnId
     */
    CompletableFuture<TxnID> endTxnOnSubscription(String topic, String subscription, long txnIdMostBits,
        long txnIdLeastBits, PulsarApi.TxnAction action);

    /**
     * Handle response of end transaction on topic.
     * @param requestId request ID
     * @param response response
     */
    void handleEndTxnOnTopicResponse(long requestId, PulsarApi.CommandEndTxnOnPartitionResponse response);

    /**
     * Handle response of tend transaction on subscription
     * @param requestId request ID
     * @param response response
     */
    void handleEndTxnOnSubscriptionResponse(long requestId, PulsarApi.CommandEndTxnOnSubscriptionResponse response);
}
