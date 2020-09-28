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
package org.apache.pulsar.broker.transaction.buffer;

import com.google.common.annotations.Beta;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * A class represents an entry appended to a transaction.
 */
@Beta
public interface TransactionEntry extends Entry, AutoCloseable {

    /**
     * The transaction id that the entry is appended to.
     *
     * @return the transaction id
     */
    TxnID txnId();

    /**
     * The sequence id of this entry in this transaction.
     *
     * @return the sequence id
     */
    long sequenceId();

    int numMessageInTxn();

    /**
     * The ledger id that the transaction is committed to.
     *
     * @return the ledger id that the transaction is committed to.
     */
    long committedAtLedgerId();

    /**
     * The entry id that the transaction is committed to.
     *
     * @return the entry id that the transaction is committed to.
     */
    long committedAtEntryId();

    /**
     * Returns the entry saved in the {@link TransactionBuffer}.
     *
     * @return the {@link Entry}.
     */
    Entry getEntry();

    /**
     * Close the entry to release the resource that it holds.
     */
    @Override
    void close();

}
