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

public class EntryStartBatchIndexes {

    private int[] startBatchIndexes = new int[100];

    public int getStartBatchIndex(int index) {
        return startBatchIndexes[index];
    }

    public void setStartBatchIndex(int index, int startBatchIndex) {
        startBatchIndexes[index] = startBatchIndex;
    }

    public void recycle() {
        handle.recycle(this);
    }

    public static EntryStartBatchIndexes get(int size) {
        EntryStartBatchIndexes instance = RECYCLER.get();
        if (instance.startBatchIndexes.length < size) {
            instance.startBatchIndexes = new int[size];
        }
        return instance;
    }

    EntryStartBatchIndexes(Recycler.Handle<EntryStartBatchIndexes> handle) {
        this.handle = handle;
    }

    private final Recycler.Handle<EntryStartBatchIndexes> handle;
    private final static Recycler<EntryStartBatchIndexes> RECYCLER = new Recycler<EntryStartBatchIndexes>() {
        @Override
        protected EntryStartBatchIndexes newObject(Handle<EntryStartBatchIndexes> handle) {
            return new EntryStartBatchIndexes(handle);
        }
    };
}
