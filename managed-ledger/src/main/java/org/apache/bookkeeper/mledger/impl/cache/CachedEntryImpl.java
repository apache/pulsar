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
package org.apache.bookkeeper.mledger.impl.cache;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.AbstractEntryImpl;

final class CachedEntryImpl extends AbstractEntryImpl<CachedEntryImpl> implements CachedEntry {
    private static final Recycler<CachedEntryImpl> RECYCLER = new Recycler<CachedEntryImpl>() {
        @Override
        protected CachedEntryImpl newObject(Handle<CachedEntryImpl> handle) {
            return new CachedEntryImpl(handle);
        }
    };

    public static CachedEntryImpl create(Position position, ByteBuf data) {
        CachedEntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = position.getLedgerId();
        entry.entryId = position.getEntryId();
        entry.setDataBuffer(data.retainedDuplicate());
        entry.setRefCnt(1);
        return entry;
    }

    private CachedEntryImpl(Recycler.Handle<CachedEntryImpl> recyclerHandle) {
        super(recyclerHandle);
    }

    @Override
    public boolean matchesKey(Position key) {
        return key != null && entryId == key.getEntryId() && ledgerId == key.getLedgerId();
    }
}
