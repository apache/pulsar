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
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.common.api.proto.PulsarApi;

import java.util.concurrent.CompletableFuture;

/**
 * The implementation of {@link TransactionBufferClient}.
 */
@Slf4j
public class TransactionBufferClientImpl implements TransactionBufferClient {

    private final TransactionBufferHandler tbHandler;

    private TransactionBufferClientImpl(TransactionBufferHandler tbHandler) {
        this.tbHandler = tbHandler;
    }

    public static TransactionBufferClient create(NamespaceService namespaceService, ConnectionPool connectionPool) {
        TransactionBufferHandler handler = new TransactionBufferHandlerImpl(connectionPool, namespaceService);
        return new TransactionBufferClientImpl(handler);
    }
    @Override
    public CompletableFuture<TxnID> commitTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits) {
        return tbHandler.endTxnOnTopic(topic, txnIdMostBits, txnIdLeastBits, PulsarApi.TxnAction.COMMIT);
    }

    @Override
    public CompletableFuture<TxnID> abortTxnOnTopic(String topic, long txnIdMostBits, long txnIdLeastBits) {
        return tbHandler.endTxnOnTopic(topic, txnIdMostBits, txnIdLeastBits, PulsarApi.TxnAction.ABORT);
    }

    @Override
    public CompletableFuture<TxnID> commitTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits) {
        return tbHandler.endTxnOnSubscription(topic, subscription, txnIdMostBits, txnIdLeastBits, PulsarApi.TxnAction.COMMIT);
    }

    @Override
    public CompletableFuture<TxnID> abortTxnOnSubscription(String topic, String subscription, long txnIdMostBits, long txnIdLeastBits) {
        return tbHandler.endTxnOnSubscription(topic, subscription, txnIdMostBits, txnIdLeastBits, PulsarApi.TxnAction.ABORT);
    }

}
