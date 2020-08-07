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
package org.apache.pulsar.broker.service.persistent;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.client.api.transaction.TxnID;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class TransactionReader {

    private TransactionBuffer transactionBuffer;
    private volatile long startSequenceId = 0;

    public void read(Topic topic, ConcurrentLinkedQueue<TxnID> transactionQueue,
                     int readMessageNum, Object ctx,
                     AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        if (transactionBuffer == null) {
            topic.getTransactionBuffer(false).whenComplete((tb, throwable) -> {
                if (throwable != null) {
                    log.error("Get transactionBuffer failed.", throwable);
                    readEntriesCallback.readEntriesFailed(
                            ManagedLedgerException.getManagedLedgerException(throwable), ctx);
                    return;
                }
                transactionBuffer = tb;
                read(transactionQueue, readMessageNum, ctx, readEntriesCallback);
            });
        } else {
            read(transactionQueue, readMessageNum, ctx, readEntriesCallback);
        }
    }

    private void read(ConcurrentLinkedQueue<TxnID> transactionQueue,
                     int readMessageNum, Object ctx,
                     AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        final TxnID txnID = transactionQueue.peek();
        transactionBuffer.openTransactionBufferReader(txnID, startSequenceId).thenAccept(reader -> {
            reader.readNext(readMessageNum).whenComplete((transactionEntries, throwable) -> {
                if (throwable != null) {
                    log.error("Read transaction messages failed.", throwable);
                    readEntriesCallback.readEntriesFailed(
                            ManagedLedgerException.getManagedLedgerException(throwable), ctx);
                    return;
                }
                if (transactionEntries == null || transactionEntries.size() < readMessageNum) {
                    startSequenceId = 0;
                    transactionQueue.remove(txnID);
                    reader.close();
                }
                List<Entry> entryList = new ArrayList<>(transactionEntries.size());
                for (int i = 0; i < transactionEntries.size(); i++) {
                    if (i == (transactionEntries.size() -1)) {
                        startSequenceId = transactionEntries.get(i).sequenceId();
                    }
                    entryList.add(transactionEntries.get(i).getEntry());
                }
                readEntriesCallback.readEntriesComplete(entryList, ctx);
            });
        }).exceptionally(throwable -> {
            log.error("Open transactionBufferReader failed.", throwable);
            readEntriesCallback.readEntriesFailed(
                    ManagedLedgerException.getManagedLedgerException(throwable), ctx);
            return null;
        });
    }

}
