/*
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
 *
 */

package org.apache.pulsar.transaction.buffer.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.transaction.buffer.TransactionBufferProvider;

public class PersistentTransactionBufferProvider implements TransactionBufferProvider {
    private final BrokerService brokerService;
    private final String topic;
    private final String txnTopic;

    public PersistentTransactionBufferProvider(BrokerService service, String topic) {
        this.brokerService = service;
        // TODO: get the transaction topic name by the TopicName.getPersistentNamingEncoding(isTxn)
        this.txnTopic = TopicName.get(topic).getPersistenceNamingEncoding() + "/_txnlog";
        this.topic = topic;
    }

    @Override
    public CompletableFuture<TransactionBuffer> newTransactionBuffer() {
        CompletableFuture<TransactionBuffer> newBufferFuture = new CompletableFuture<>();
        brokerService.getManagedLedgerConfig(TopicName.get(topic)).thenAccept(config -> {
            config.setCreateIfMissing(true);
            brokerService.getManagedLedgerFactory()
                         .asyncOpen(txnTopic, config, new AsyncCallbacks.OpenLedgerCallback() {
                             @Override
                             public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                                 try {
                                     PersistentTransactionBuffer buffer =
                                         new PersistentTransactionBuffer(txnTopic, ledger, brokerService);
                                     newBufferFuture.complete(buffer);
                                 } catch (Exception e) {
                                     newBufferFuture.completeExceptionally(e);
                                 }
                             }

                             @Override
                             public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                                 newBufferFuture.completeExceptionally(exception);
                             }
                         }, null);
        });
        return newBufferFuture;
    }
}
