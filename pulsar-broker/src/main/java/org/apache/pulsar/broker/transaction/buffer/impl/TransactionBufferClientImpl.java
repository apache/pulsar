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

import io.netty.util.HashedWheelTimer;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.common.api.proto.TxnAction;

/**
 * The implementation of {@link TransactionBufferClient}.
 */
@Slf4j
public class TransactionBufferClientImpl implements TransactionBufferClient {

    private final TransactionBufferHandler tbHandler;

    private TransactionBufferClientImpl(TransactionBufferHandler tbHandler) {
        this.tbHandler = tbHandler;
    }

    public static TransactionBufferClient create(PulsarClient pulsarClient, HashedWheelTimer timer) {
        TransactionBufferHandler handler = new TransactionBufferHandlerImpl(pulsarClient, timer);
        return new TransactionBufferClientImpl(handler);
    }

    @Override
    public CompletableFuture<TxnID> commitTxnOnTopic(String topic, long txnIdMostBits,
                                                     long txnIdLeastBits, long lowWaterMark) {
        return tbHandler.endTxnOnTopic(topic, txnIdMostBits, txnIdLeastBits, TxnAction.COMMIT, lowWaterMark);
    }

    @Override
    public CompletableFuture<TxnID> abortTxnOnTopic(String topic, long txnIdMostBits,
                                                    long txnIdLeastBits, long lowWaterMark) {
        return tbHandler.endTxnOnTopic(topic, txnIdMostBits, txnIdLeastBits, TxnAction.ABORT, lowWaterMark);
    }

    @Override
    public CompletableFuture<TxnID> commitTxnOnSubscription(String topic, String subscription, long txnIdMostBits,
                                                            long txnIdLeastBits, long lowWaterMark) {
        return tbHandler.endTxnOnSubscription(topic, subscription, txnIdMostBits, txnIdLeastBits,
                TxnAction.COMMIT, lowWaterMark);
    }

    @Override
    public CompletableFuture<TxnID> abortTxnOnSubscription(String topic, String subscription,
                                                           long txnIdMostBits, long txnIdLeastBits, long lowWaterMark) {
        return tbHandler.endTxnOnSubscription(topic, subscription, txnIdMostBits, txnIdLeastBits,
                TxnAction.ABORT, lowWaterMark);
    }

    @Override
    public void close() {
        tbHandler.close();
    }
}
