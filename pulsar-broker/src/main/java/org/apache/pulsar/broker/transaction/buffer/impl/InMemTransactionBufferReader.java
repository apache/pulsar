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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionEntry;
import org.apache.pulsar.broker.transaction.buffer.exceptions.EndOfTransactionException;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * A {@link TransactionBufferReader} implementation that reads entries from {@link InMemTransactionBuffer}.
 */
public class InMemTransactionBufferReader implements TransactionBufferReader {

    private final TxnID txnId;
    private final Iterator<Entry<Long, ByteBuf>> entries;
    private final long committedAtLedgerId;
    private final long committedAtEntryId;

    // the iterator should hold the references to the entries
    // so when the reader is closed, all the entries can be released.
    public InMemTransactionBufferReader(TxnID txnId, Iterator<Entry<Long, ByteBuf>> entries, long committedAtLedgerId,
                                        long committedAtEntryId) {
        this.txnId = txnId;
        this.entries = entries;
        this.committedAtLedgerId = committedAtLedgerId;
        this.committedAtEntryId = committedAtEntryId;
    }

    @Override
    public synchronized CompletableFuture<List<TransactionEntry>> readNext(int numEntries) {
        CompletableFuture<List<TransactionEntry>> readFuture = new CompletableFuture<>();

        if (numEntries <= 0) {
            readFuture.completeExceptionally(new IllegalArgumentException(
                "`numEntries` should be larger than 0"
            ));
            return readFuture;
        }

        List<TransactionEntry> txnEntries = new ArrayList<>(numEntries);
        int i = 0;
        while (i < numEntries && entries.hasNext()) {
            Entry<Long, ByteBuf> entry = entries.next();
            TransactionEntry txnEntry = new TransactionEntryImpl(
                txnId,
                entry.getKey(),
                EntryImpl.create(-1L, -1L, entry.getValue()),
                committedAtLedgerId,
                committedAtEntryId,
                -1
            );
            txnEntries.add(txnEntry);
            ++i;
        }

        if (txnEntries.isEmpty()) {
            readFuture.completeExceptionally(new EndOfTransactionException(
                "No more entries found in transaction `" + txnId + "`"
            ));
        } else {
            readFuture.complete(txnEntries);
        }
        return readFuture;
    }

    @Override
    public synchronized void close() {
        while (entries.hasNext()) {
            Entry<Long, ByteBuf> entry = entries.next();
            entry.getValue().release();
        }
    }
}
