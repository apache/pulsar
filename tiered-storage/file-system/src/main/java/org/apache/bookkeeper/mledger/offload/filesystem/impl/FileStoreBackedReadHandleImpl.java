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
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;

import org.apache.bookkeeper.mledger.offload.filesystem.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.offload.filesystem.OffloadIndexFile;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class FileStoreBackedReadHandleImpl implements ReadHandle {
    private static final Logger log = LoggerFactory.getLogger(FileStoreBackedReadHandleImpl.class);
    private final long ledgerId;
    private final ExecutorService executor;
    private final OffloadIndexFile offloadIndexFile;
    private final FSDataInputStream dataInputStream;

    private FileStoreBackedReadHandleImpl(ExecutorService executor, long ledgerId,
                                          OffloadIndexFile offloadIndexFile, FSDataInputStream dataInputStream) {
        this.ledgerId = ledgerId;
        this.executor = executor;
        this.offloadIndexFile = offloadIndexFile;
        this.dataInputStream = dataInputStream;
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return offloadIndexFile.getLedgerMetadata();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executor.submit(() -> {
                try {
                    dataInputStream.close();
                    promise.complete(null);
                } catch (IOException t) {
                    promise.completeExceptionally(t);
                }
            });
        return promise;
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        log.debug("Ledger {}: reading {} - {}", getId(), firstEntry, lastEntry);
        CompletableFuture<LedgerEntries> promise = new CompletableFuture<>();
        executor.submit(() -> {
                if (firstEntry > lastEntry
                    || firstEntry < 0
                    || lastEntry > getLastAddConfirmed()) {
                    promise.completeExceptionally(new BKException.BKIncorrectParameterException());
                    return;
                }
                long entriesToRead = (lastEntry - firstEntry) + 1;
                List<LedgerEntry> entries = new ArrayList<LedgerEntry>();
                long nextExpectedId = firstEntry;
                try {
                    OffloadIndexEntry floorEntry = offloadIndexFile.getFloorIndexEntryByEntryId(firstEntry);
                    OffloadIndexEntry ceilingEntry = offloadIndexFile.getCeilingIndexEntryByEntryId(lastEntry);
                    dataInputStream.seek(floorEntry.getOffset());
                    if (floorEntry.getEntryId() != ceilingEntry.getEntryId()) {
                        entriesToRead = lastEntry == ceilingEntry.getEntryId() ? entriesToRead - 1 : entriesToRead;
                        int readBytesSize = ceilingEntry.getOffset() - floorEntry.getOffset();
                        byte[] readBytes = new byte[readBytesSize];
                        dataInputStream.read(readBytes);
                        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(readBytesSize, readBytesSize);
                        buf.writeBytes(readBytes);
                        while (entriesToRead > 0) {
                            int length = buf.readInt();
                            long entryId = buf.readLong();
                            if (entryId == nextExpectedId) {
                                ByteBuf entryBuf = PooledByteBufAllocator.DEFAULT.buffer(length, length);
                                entries.add(LedgerEntryImpl.create(ledgerId, entryId, length, entryBuf));
                                buf.readBytes(entryBuf);
                                entriesToRead--;
                                nextExpectedId++;
                            } else if (entryId > lastEntry) {
                                log.info("Expected to read {}, but read {}, which is greater than last entry {}",
                                        nextExpectedId, entryId, lastEntry);
                                throw new BKException.BKUnexpectedConditionException();
                            } else {
                                buf.readBytes(length);
                            }
                        }
                        if (lastEntry == ceilingEntry.getEntryId()) {
                            int length = dataInputStream.readInt();
                            long entryId = dataInputStream.readLong();
                            byte[] entryByte = new byte[length];
                            dataInputStream.read(entryByte);
                            ByteBuf entryBuf = PooledByteBufAllocator.DEFAULT.buffer(length, length);
                            entryBuf.writeBytes(entryByte);
                            entries.add(LedgerEntryImpl.create(ledgerId, entryId, length, entryBuf));
                        }

                    } else {
                        int length = dataInputStream.readInt();
                        long entryId = dataInputStream.readLong();
                        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(length, length);
                        entries.add(LedgerEntryImpl.create(ledgerId, entryId, length, buf));
                        int toWrite = length;
                        while (toWrite > 0) {
                            toWrite -= buf.writeBytes(dataInputStream, toWrite);
                        }

                    }

                    promise.complete(LedgerEntriesImpl.create(entries));
                } catch (Throwable t) {
                    promise.completeExceptionally(t);
                    entries.forEach(LedgerEntry::close);
                }
            });
        return promise;
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public long getLastAddConfirmed() {
        return getLedgerMetadata().getLastEntryId();
    }

    @Override
    public long getLength() {
        return getLedgerMetadata().getLength();
    }

    @Override
    public boolean isClosed() {
        return getLedgerMetadata().isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        CompletableFuture<LastConfirmedAndEntry> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    public static ReadHandle open(ScheduledExecutorService executor, String dataFilePath,
                                  String indexFilePath, FileSystem fileSystem, long ledgerId) throws IOException {
            OffloadIndexFile offloadIndexFile =
                    OffloadIndexFileImpl.getRecycler().get()
                            .initIndexFile(fileSystem.open(new Path(indexFilePath)));
            return new FileStoreBackedReadHandleImpl(executor, ledgerId,
                    offloadIndexFile, fileSystem.open(new Path(dataFilePath)));
    }
}
