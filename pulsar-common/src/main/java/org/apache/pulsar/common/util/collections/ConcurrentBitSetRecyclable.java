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
package org.apache.pulsar.common.util.collections;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import lombok.EqualsAndHashCode;

import java.util.BitSet;

/**
 * Safe multithreaded version of {@code BitSet} and leverage netty recycler.
 */
@EqualsAndHashCode(callSuper = true)
public class ConcurrentBitSetRecyclable extends ConcurrentBitSet {

    private final Handle<ConcurrentBitSetRecyclable> recyclerHandle;

    private static final Recycler<ConcurrentBitSetRecyclable> RECYCLER = new Recycler<ConcurrentBitSetRecyclable>() {
        protected ConcurrentBitSetRecyclable newObject(Handle<ConcurrentBitSetRecyclable> recyclerHandle) {
            return new ConcurrentBitSetRecyclable(recyclerHandle);
        }
    };

    private ConcurrentBitSetRecyclable(Handle<ConcurrentBitSetRecyclable> recyclerHandle) {
        super();
        this.recyclerHandle = recyclerHandle;
    }

    public static ConcurrentBitSetRecyclable create() {
        return RECYCLER.get();
    }

    public static ConcurrentBitSetRecyclable create(BitSet bitSet) {
        ConcurrentBitSetRecyclable recyclable = RECYCLER.get();
        recyclable.or(bitSet);
        return recyclable;
    }

    public void recycle() {
        this.clear();
        recyclerHandle.recycle(this);
    }
}
