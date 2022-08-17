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

import java.util.Map;
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
     * @param properties properties
     * @return
     */
    CompletableFuture<?> appendPendingDeleteLedger(String topicName, long ledgerId, LedgerInfo context,
                                                   LedgerComponent component, LedgerType type, Map<String, String> properties);

    /**
     *
     * @param topicName topicName
     * @param ledgerId  ledgerId
     * @param component managed_ledger, managed_cursor, schema_storage
     * @param isBelievedDelete isBelievedDelete, if false, we should check the param is match the ledger metadata.
     * @return
     */
    CompletableFuture<?> asyncDeleteLedger(String topicName, long ledgerId, LedgerComponent component,
                                           boolean isBelievedDelete);

    /**
     *
     * @param topicName topicName
     * @param ledgerId ledgerId
     * @param offloadContext offloadContext
     * @return
     */
    CompletableFuture<?> asyncDeleteOffloadedLedger(String topicName, long ledgerId,
                                                    MLDataFormats.OffloadContext offloadContext);

    /**
     * Close.
     */
    void close() throws Exception;

    /**
     * Async close.
     */
    CompletableFuture<?> asyncClose();

    boolean isTopicTwoPhaseDeletionEnabled();

    class LedgerDeletionServiceDisable implements LedgerDeletionService {

        @Override
        public void start() {
            //No op
        }

        @Override
        public CompletableFuture<?> appendPendingDeleteLedger(String topicName, long ledgerId, LedgerInfo context,
                                                              LedgerComponent component, LedgerType type,
                                                              Map<String, String> properties) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<?> asyncDeleteLedger(String topicName, long ledgerId, LedgerComponent component,
                                                      boolean isBelievedDelete) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<?> asyncDeleteOffloadedLedger(String topicName, long ledgerId,
                                                               MLDataFormats.OffloadContext offloadContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws Exception {
            //No op
        }

        @Override
        public CompletableFuture<?> asyncClose() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean isTopicTwoPhaseDeletionEnabled() {
            return false;
        }
    }

}
