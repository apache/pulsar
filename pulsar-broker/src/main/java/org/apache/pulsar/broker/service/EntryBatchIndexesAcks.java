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
import org.apache.pulsar.common.api.proto.PulsarApi.IntRange;

import java.util.ArrayList;
import java.util.List;

public class EntryBatchIndexesAcks {

    List<Pair<Integer, List<IntRange>>> indexesAcks = new ArrayList<>();

    public void setIndexesAcks(Pair<Integer, List<IntRange>> indexesAcks) {
        this.indexesAcks.add(indexesAcks);
    }

    public Pair<Integer, List<IntRange>> getIndexesAcks(int entryIdx) {
        return this.indexesAcks.get(entryIdx);
    }

    public int getTotalAckedIndexCount() {
        int count = 0;
        for (Pair<Integer, List<IntRange>> pair : indexesAcks) {
            if (pair != null) {
                count += pair.getLeft();
            }
        }
        return count;
    }

    public void recycle() {
        this.indexesAcks.clear();
        handle.recycle(this);
    }

    public static EntryBatchIndexesAcks get() {
        return RECYCLER.get();
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
