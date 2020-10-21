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
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionBufferProviderException;
import org.apache.pulsar.common.naming.TopicName;

import java.util.concurrent.CompletableFuture;

/**
 * Persistent transaction buffer provider.
 */
@Slf4j
public class PersistentTransactionBufferProvider implements TransactionBufferProvider {

    @Override
    public CompletableFuture<TransactionBuffer> newTransactionBuffer() {
        return null;
    }

    @Override
    public CompletableFuture<TransactionBuffer> newTransactionBuffer(Topic originTopic) {
        CompletableFuture<TransactionBuffer> tbFuture = new CompletableFuture<>();

        if (originTopic == null) {
            tbFuture.completeExceptionally(new TransactionBufferProviderException("The originTopic is null."));
            return tbFuture;
        }
        if (!(originTopic instanceof PersistentTopic)) {
            tbFuture.completeExceptionally(new TransactionBufferProviderException(
                    "The originTopic is not persistentTopic."));
            return tbFuture;
        }

        PersistentTopic originPersistentTopic = (PersistentTopic) originTopic;
        String tbTopicName = PersistentTransactionBuffer.getTransactionBufferTopicName(originPersistentTopic.getName());

        originPersistentTopic.getBrokerService().getManagedLedgerFactory()
            .asyncOpen(TopicName.get(tbTopicName).getPersistenceNamingEncoding(),
                    new AsyncCallbacks.OpenLedgerCallback() {
                        @Override
                        public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                            try {
                                tbFuture.complete(new PersistentTransactionBuffer(tbTopicName, ledger,
                                        originPersistentTopic.getBrokerService(), originTopic));
                            } catch (Exception e) {
                                log.error("New PersistentTransactionBuffer error.", e);
                                tbFuture.completeExceptionally(e);
                            }
                        }

                        @Override
                        public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            log.error("Open transactionBuffer managedLedger failed.", exception);
                            tbFuture.completeExceptionally(exception);
                        }
                    }, null);
        return tbFuture;
    }

}
