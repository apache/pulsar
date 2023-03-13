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
package org.apache.bookkeeper.mledger.intercept;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;

/**
 * Interceptor for ManagedLedger.
 * */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ManagedLedgerInterceptor {

    /**
     * Intercept an OpAddEntry and return an OpAddEntry.
     * @param op an OpAddEntry to be intercepted.
     * @param numberOfMessages
     * @return an OpAddEntry.
     */
    OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages);

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
     * Intercept when ManagedLedger is initialized.
     * @param name name of ManagedLedger
     * @param ledgerHandle a LedgerHandle.
     */
    CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LedgerHandle ledgerHandle);

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
     * @param ledgerWriteOp OpAddEntry used to trigger ledger write.
     * @param dataToBeStoredInLedger data to be stored in ledger
     * @return handle to the processor
     */
    default PayloadProcessorHandle processPayloadBeforeLedgerWrite(OpAddEntry ledgerWriteOp,
                                                                   ByteBuf dataToBeStoredInLedger){
        return null;
    }
}
