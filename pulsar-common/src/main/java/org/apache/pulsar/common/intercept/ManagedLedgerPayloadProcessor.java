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
package org.apache.pulsar.common.intercept;

import io.netty.buffer.ByteBuf;

public interface ManagedLedgerPayloadProcessor {
    interface Processor {
        /**
         * Process the input payload and return a new payload
         * NOTE: If this processor returns a different ByteBuf instance than the passed one
         *       DO THE FOLLOWING to avoid memory leaks
         *       1. Call inputPayload.release() to release a reference
         *       2. Call retain() on the ByteBuf that is being returned
         * @param contextObj context object
         * @param inputPayload The input payload buffer
         * @return processed data
         */
        ByteBuf process(Object contextObj, ByteBuf inputPayload);

        /**
         * To release any resource used during the process.
         * NOTE: To avoid memory leak, do the following ONLY if a different ByteBuf instance was returned in process()
         *       1. Call processedPayload.release() to release a reference
         *       2. Do any other cleanup needed
         *
         * @param processedPayload The processed payload that was returned in process() call
         */
        void release(ByteBuf processedPayload);
    }
    /**
     * Used by ManagedLedger for pre-processing payload before storing in bookkeeper ledger
     * @return Handle to Processor instance
     */
    default Processor inputProcessor() {
        return null;
    }

    /**
     * Used by ManagedLedger for processing payload after reading from bookkeeper ledger
     * @return Handle to Processor instance
     */
    default Processor outputProcessor() {
        return null;
    }
}
