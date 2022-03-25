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

import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;

public interface TopicTransactionBufferRecoverCallBack {

    /**
     * Topic transaction buffer recover complete.
     */
    void recoverComplete();

    /**
     * No message with transaction has ever been sent.
     * Skip recovery procedure
     */
    void noNeedToRecover();

    /**
     * Handle transactionBufferSnapshot.
     *
     * @param snapshot the transaction buffer snapshot
     */
    void handleSnapshot(TransactionBufferSnapshot snapshot);

    /**
     * Handle transaction entry beyond the snapshot.
     *
     * @param entry the transaction message entry
     */
    void handleTxnEntry(Entry entry);

    /**
     * Topic transaction buffer recover exceptionally.
     */
    void recoverExceptionally(Throwable e);
}
