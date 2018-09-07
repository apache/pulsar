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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.InputStream;

import org.apache.bookkeeper.client.impl.LedgerEntryImpl;

/**
 * Mocked BK {@link LedgerEntry}. Used by {@link MockLedgerHandle}.
 */
public class MockLedgerEntry extends LedgerEntry {

    final long ledgerId;
    final long entryId;
    final byte[] data;

    public MockLedgerEntry(long ledgerId, long entryId, byte[] data) {
        super(LedgerEntryImpl.create(ledgerId, entryId, data.length, Unpooled.wrappedBuffer(data)));
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.data = data;
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    @Override
    public long getLength() {
        return data.length;
    }

    @Override
    public byte[] getEntry() {
        return data;
    }

    @Override
    public ByteBuf getEntryBuffer() {
        return Unpooled.wrappedBuffer(data);
    }

    @Override
    public InputStream getEntryInputStream() {
        return null;
    }

}
