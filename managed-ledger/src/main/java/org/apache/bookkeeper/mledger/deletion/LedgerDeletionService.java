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
package org.apache.bookkeeper.mledger.deletion;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

public interface LedgerDeletionService {

    /**
     * Start.
     */
    void start() throws PulsarClientException, PulsarAdminException;

    /**
     * @param topicName topicName
     * @param ledgerId  ledgerId
     * @param context   ledgerInfo
     * @param component managed_ledger, managed_cursor, schema_storage
     * @param type      ledger, offload_ledger
     * @return
     */
    CompletableFuture<?> appendPendingDeleteLedger(String topicName, long ledgerId, LedgerInfo context,
                                                   LedgerComponent component, LedgerType type);


    CompletableFuture<?> asyncDeleteLedger(long ledgerId, LedgerComponent ledgerComponent, String topicName,
                                           boolean isBelievedDelete);


    CompletableFuture<?> asyncDeleteOffloadedLedger(long ledgerId, String topicName,
                                                    MLDataFormats.OffloadContext ledgerInfo);

    /**
     * Close.
     */
    void close() throws Exception;

    /**
     * Async close.
     */
    CompletableFuture<?> asyncClose();

    class LedgerDeletionServiceDisable implements LedgerDeletionService {

        @Override
        public void start() {
            //No op
        }

        private static final CompletableFuture<?> COMPLETABLE_FUTURE = CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<?> appendPendingDeleteLedger(String topicName, long ledgerId, LedgerInfo context,
                                                              LedgerComponent component, LedgerType type) {
            return COMPLETABLE_FUTURE;
        }

        @Override
        public CompletableFuture<?> asyncDeleteLedger(long ledgerId, LedgerComponent ledgerComponent, String topicName,
                                                      boolean isBelievedDelete) {
            return COMPLETABLE_FUTURE;
        }

        @Override
        public CompletableFuture<?> asyncDeleteOffloadedLedger(long ledgerId, String topicName,
                                                               MLDataFormats.OffloadContext ledgerInfo) {
            return COMPLETABLE_FUTURE;
        }

        @Override
        public void close() throws Exception {
            //No op
        }

        @Override
        public CompletableFuture<?> asyncClose() {
            return COMPLETABLE_FUTURE;
        }
    }

}
