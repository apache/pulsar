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
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.transaction.buffer.TransactionEntry;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * A simple implementation of {@link TransactionEntry}.
 */
public class TransactionEntryImpl implements TransactionEntry {

    private final TxnID txnId;
    private final long sequenceId;
    private final long committedAtLedgerId;
    private final long committedAtEntryId;
    private final Entry entry;
    private final int numMessageInTxn;
    private int startBatchIndex;

    public TransactionEntryImpl(TxnID txnId,
                                long sequenceId,
                                Entry entry,
                                long committedAtLedgerId,
                                long committedAtEntryId,
                                int numMessageInTxn) {
        this.txnId = txnId;
        this.sequenceId = sequenceId;
        this.entry = entry;
        this.committedAtLedgerId = committedAtLedgerId;
        this.committedAtEntryId = committedAtEntryId;
        this.numMessageInTxn = numMessageInTxn;
    }

    @Override
    public TxnID txnId() {
        return txnId;
    }

    @Override
    public long sequenceId() {
        return sequenceId;
    }

    @Override
    public int numMessageInTxn() {
        return numMessageInTxn;
    }

    @Override
    public long committedAtLedgerId() {
        return committedAtLedgerId;
    }

    @Override
    public long committedAtEntryId() {
        return committedAtEntryId;
    }

    @Override
    public Entry getEntry() {
        return entry;
    }

    public void setStartBatchIndex(int startBatchIndex) {
        this.startBatchIndex = startBatchIndex;
    }

    public int getStartBatchIndex() {
        return startBatchIndex;
    }

    @Override
    public void close() {
        if (null != entry) {
            entry.getDataBuffer().release();
            entry.release();
        }
    }

    @Override
    public byte[] getData() {
        return entry.getData();
    }

    @Override
    public byte[] getDataAndRelease() {
        return entry.getDataAndRelease();
    }

    @Override
    public int getLength() {
        return entry.getLength();
    }

    @Override
    public ByteBuf getDataBuffer() {
        return entry.getDataBuffer();
    }

    @Override
    public Position getPosition() {
        return entry.getPosition();
    }

    @Override
    public long getLedgerId() {
        return committedAtLedgerId;
    }

    @Override
    public long getEntryId() {
        return committedAtEntryId;
    }

    @Override
    public boolean release() {
        return this.entry.release();
    }
}
