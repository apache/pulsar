/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.buffer.impl;

import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

@Getter
@Setter
@AllArgsConstructor
@Builder
public class TransactionMetaImpl implements TransactionMeta{

    private final TxnID txnID;
    private final SortedMap<Long, Position> entries;
    private TxnStatus txnStatus;
    private long committedAtLedgerId = -1L;
    private long committedAtEntryId = -1L;

    TransactionMetaImpl(TxnID txnID) {
        this.txnID = txnID;
        this.entries = new TreeMap<>();
        this.txnStatus = TxnStatus.OPEN;
    }

    @Override
    public TxnID id() {
        return this.txnID;
    }

    @Override
    public synchronized TxnStatus status() {
        return this.txnStatus;
    }

    @Override
    public int numEntries() {
        synchronized (entries) {
            return entries.size();
        }
    }
}
