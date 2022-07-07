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
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

public interface LedgerDeletionService {

    /**
     *
     */
    void start() throws PulsarClientException, PulsarAdminException;


    CompletableFuture<?> appendRubbishLedger(String topicName, long ledgerId, LedgerInfo context, LedgerComponent source,
                                             LedgerType type, boolean checkLedgerStillInUse);

    void close() throws Exception;

    /**
     * Async close managedTrash, it will persist trash data to meta store.
     *
     * @return
     */
    CompletableFuture<?> asyncClose();

    void setUpOffloadConfig(ManagedLedgerConfig managedLedgerConfig);

    class LedgerDeletionServiceDisable implements LedgerDeletionService {

        @Override
        public void start() {
            //No op
        }

        private static final CompletableFuture<?> COMPLETABLE_FUTURE = CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<?> appendRubbishLedger(String topicName, long ledgerId, LedgerInfo context,
                                                        LedgerComponent source, LedgerType type,
                                                        boolean checkLedgerStillInUse) {
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

        @Override
        public void setUpOffloadConfig(ManagedLedgerConfig managedLedgerConfig) {
            //No op
        }
    }

}
