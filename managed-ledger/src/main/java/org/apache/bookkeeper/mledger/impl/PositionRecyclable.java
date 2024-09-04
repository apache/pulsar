/*
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
package org.apache.bookkeeper.mledger.impl;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import org.apache.bookkeeper.mledger.Position;

/**
 * Recyclable implementation of Position that is used to reduce the overhead of creating new Position objects.
 */
public class PositionRecyclable implements Position {
    private final Handle<PositionRecyclable> recyclerHandle;

    private static final Recycler<PositionRecyclable> RECYCLER = new Recycler<PositionRecyclable>() {
        @Override
        protected PositionRecyclable newObject(Recycler.Handle<PositionRecyclable> recyclerHandle) {
            return new PositionRecyclable(recyclerHandle);
        }
    };

    private long ledgerId;
    private long entryId;

    private PositionRecyclable(Handle<PositionRecyclable> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    public void recycle() {
        ledgerId = -1;
        entryId = -1;
        recyclerHandle.recycle(this);
    }

    @Override
    public int hashCode() {
        return hashCodeForPosition();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Position && compareTo((Position) obj) == 0;
    }

    public static PositionRecyclable get(long ledgerId, long entryId) {
        PositionRecyclable position = RECYCLER.get();
        position.ledgerId = ledgerId;
        position.entryId = entryId;
        return position;
    }
}