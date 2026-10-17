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
 */
package org.apache.bookkeeper.mledger.intercept;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.Entry;

/**
 * Interceptor for ManagedLedger.
 * */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ManagedLedgerInterceptor {
    /**
     * An operation to add an entry to a ledger.
     */
    interface AddEntryOperation {
        /**
         * Get the data to be written to the ledger.
         * @return data to be written to the ledger
         */
        ByteBuf getData();
        /**
         * Set the data to be written to the ledger.
         * @param data data to be written to the ledger
         */
        void setData(ByteBuf data);
        /**
         * Get the operation context object.
         * @return context the context object
         */
        Object getCtx();
    }

    /**
     * Intercept adding an entry to a ledger.
     *
     * @param op an operation to be intercepted.
     * @param numberOfMessages
     */
    void beforeAddEntry(AddEntryOperation op, int numberOfMessages);

    /**
     * Intercept When add entry failed.
     * @param numberOfMessages
     */
    default void afterFailedAddEntry(int numberOfMessages){

    }

    /**
     * Intercept when ManagedLedger is initialized.
     * @param propertiesMap map of properties.
     */
    void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap);

    /**
     * A handle for reading the last ledger entry.
     */
    interface LastEntryHandle {
        /**
         * Read the last entry from the ledger.
         * The caller is responsible for releasing the entry.
         * @return the last entry from the ledger, if any
         */
        CompletableFuture<Optional<Entry>> readLastEntryAsync();
    }

    /**
     * Intercept when ManagedLedger is initialized.
     *
     * @param name            name of ManagedLedger
     * @param lastEntryHandle a LedgerHandle.
     */
    CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LastEntryHandle lastEntryHandle);

    /**
     * @param propertiesMap  map of properties.
     */
    void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap);

    /**
     * A reference handle to the payload processor.
     */
    interface PayloadProcessorHandle {
        /**
         * To obtain the processed data.
         * @return processed data
         */
        ByteBuf getProcessedPayload();

        /**
         * To release resources used in processor, if any.
         */
        void release();
    }
    /**
     * Intercept after entry is read from ledger, before it gets cached.
     * @param dataReadFromLedger data from ledger
     * @return handle to the processor
     */
    default PayloadProcessorHandle processPayloadBeforeEntryCache(ByteBuf dataReadFromLedger){
        return null;
    }

    /**
     * Intercept before payload gets written to ledger.
     * @param ctx the operation context object
     * @param dataToBeStoredInLedger data to be stored in ledger
     * @return handle to the processor
     */
    default PayloadProcessorHandle processPayloadBeforeLedgerWrite(Object ctx,
                                                                   ByteBuf dataToBeStoredInLedger) {
        return null;
    }
}
