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

public class EntryBatchSizes {
    private int[] sizes = new int[100];

    public int getBatchSize(int entryIdx) {
        return sizes[entryIdx];
    }

    public int setBatchSize(int entryIdx, int batchSize) {
        return sizes[entryIdx] = batchSize;
    }

    public void recyle() {
        handle.recycle(this);
    }

    public static EntryBatchSizes get(int entriesListSize) {
        EntryBatchSizes ebs = RECYCLER.get();

        if (ebs.sizes.length < entriesListSize) {
            ebs.sizes = new int[entriesListSize];
        }
        return ebs;
    }

    private EntryBatchSizes(Recycler.Handle<EntryBatchSizes> handle) {
        this.handle = handle;
    }

    private final Recycler.Handle<EntryBatchSizes> handle;
    private static final Recycler<EntryBatchSizes> RECYCLER = new Recycler<EntryBatchSizes>() {
        @Override
        protected EntryBatchSizes newObject(Handle<EntryBatchSizes> handle) {
            return new EntryBatchSizes(handle);
        }
    };
}
