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
package org.apache.bookkeeper.mledger.offload.filesystem.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.offload.filesystem.FileSystemEntryBytesReader;
import org.apache.bookkeeper.mledger.offload.filesystem.OffloadIndexFileBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FileSystemEntryBytesReaderImpl extends FileSystemEntryBytesReader {

    private static final Logger log = LoggerFactory.getLogger(FileSystemEntryBytesReaderImpl.class);
    private OffloadIndexFileBuilder builder;

    public FileSystemEntryBytesReaderImpl(ReadHandle readHandle, Map<String, String> configMap, OffloadIndexFileBuilder builder) {
        super(readHandle, configMap);
        this.builder = builder;
        builder.addIndex(0, HEADER_SIZE);

    }

    @Override
    public ByteBuf readEntries() throws IOException {
        long end = Math.min(haveOffloadEntryCount + ENTRIES_PER_READ - 1, readHandle.getLastAddConfirmed());
        canContinueRead = end != readHandle.getLastAddConfirmed();
        try (LedgerEntries ledgerEntriesOnce = readHandle.readAsync(haveOffloadEntryCount, end).get()) {
            log.debug("read ledger entries. start: {}, end: {}", haveOffloadEntryCount, end);
            haveOffloadEntryCount  = end + 1;
            Iterator<LedgerEntry> iterator = ledgerEntriesOnce.iterator();
            //when this reach ADD_INDEX_PER_WRITTEN_COUNT, add index
            int reachAddIndexCount = 0;
            //when this reach ADD_INDEX_PER_WRITTEN_BYTES_SIZE, add index
            int reachAddIndexBytesSize = 0;
            ByteBuf entryBuf = PooledByteBufAllocator.DEFAULT.buffer(1024 * 1024, 1024 * 1024 * 64);
            while (iterator.hasNext()) {
                LedgerEntry entry = iterator.next();
                ByteBuf buf = entry.getEntryBuffer().retain();
                int entryLength = buf.readableBytes();
                long entryId = entry.getEntryId();
                entryBuf.writeInt(entryLength).writeLong(entryId);
                entryBuf.writeBytes(buf);
                int entryWrittenSize = ENTRY_HEADER_SIZE + entryLength;
                reachAddIndexCount++;
                reachAddIndexBytesSize += entryWrittenSize;
                if (reachAddIndexBytesSize >= ADD_INDEX_PER_WRITTEN_BYTES_SIZE || reachAddIndexCount >= ADD_INDEX_PER_WRITTEN_COUNT) {
                    builder.addIndex(entryId, haveWrittenBytes);
                    reachAddIndexBytesSize = 0;
                    reachAddIndexCount = 0;
                }
                haveWrittenBytes += entryWrittenSize;
            }
            return entryBuf;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception when get CompletableFuture<LedgerEntries>. ", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IOException(e);
        }

    }
}
