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
package org.apache.pulsar.broker.service;


import io.netty.util.Recycler;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class EntryBatchIndexesAcks {

    Pair<Integer, long[]>[] indexesAcks = new Pair[100];

    public void setIndexesAcks(int entryIdx, Pair<Integer, long[]> indexesAcks) {
        this.indexesAcks[entryIdx] = indexesAcks;
    }

    public long[] getAckSet(int entryIdx) {
        Pair<Integer, long[]> pair = indexesAcks[entryIdx];
        return pair == null ? null : pair.getRight();
    }

    public int getTotalAckedIndexCount() {
        int count = 0;
        for (Pair<Integer, long[]> pair : indexesAcks) {
            if (pair != null) {
                count += pair.getLeft() - BitSet.valueOf(pair.getRight()).cardinality();
            }
        }
        return count;
    }

    public void recycle() {
        handle.recycle(this);
    }

    public static EntryBatchIndexesAcks get(int entriesListSize) {
        EntryBatchIndexesAcks ebi = RECYCLER.get();

        if (ebi.indexesAcks.length < entriesListSize) {
            ebi.indexesAcks = new Pair[entriesListSize];
        }
        return ebi;
    }

    private EntryBatchIndexesAcks(Recycler.Handle<EntryBatchIndexesAcks> handle) {
        this.handle = handle;
    }

    private final Recycler.Handle<EntryBatchIndexesAcks> handle;
    private static final Recycler<EntryBatchIndexesAcks> RECYCLER = new Recycler<EntryBatchIndexesAcks>() {
        @Override
        protected EntryBatchIndexesAcks newObject(Handle<EntryBatchIndexesAcks> handle) {
            return new EntryBatchIndexesAcks(handle);
        }
    };
}
