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

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class FileStoreBackedReadHandleImpl implements ReadHandle {
    private static final Logger log = LoggerFactory.getLogger(FileStoreBackedReadHandleImpl.class);
    private final ExecutorService executor;
    private final MapFile.Reader reader;
    private final long ledgerId;
    private final LedgerMetadata ledgerMetadata;

    private FileStoreBackedReadHandleImpl(ExecutorService executor, MapFile.Reader reader, long ledgerId) throws IOException {
        this.ledgerId = ledgerId;
        this.executor = executor;
        this.reader = reader;
        LongWritable key = new LongWritable();
        BytesWritable value = new BytesWritable();
        try {
            key.set(FileSystemManagedLedgerOffloader.METADATA_KEY_INDEX);
            reader.get(key, value);
            this.ledgerMetadata = parseLedgerMetadata(value.copyBytes());
        } catch (IOException e) {
            log.error("Fail to read LedgerMetadata for ledgerId {}",
                    ledgerId);
            throw new IOException("Fail to read LedgerMetadata for ledgerId " + key.get());
        }
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return ledgerMetadata;

    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executor.submit(() -> {
                try {
                    reader.close();
                } catch (IOException t) {
                    promise.completeExceptionally(t);
                }
            });
        return promise;
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        if (log.isDebugEnabled()) {
            log.debug("Ledger {}: reading {} - {}", getId(), firstEntry, lastEntry);
        }
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
            LongWritable key = new LongWritable();
            BytesWritable value = new BytesWritable();
            try {
                key.set(nextExpectedId - 1);
                reader.seek(key);
                while (entriesToRead > 0) {
                    reader.next(key, value);
                    int length = value.getLength();
                    long entryId = key.get();
                    if (entryId == nextExpectedId) {
                        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(length, length);
                        entries.add(LedgerEntryImpl.create(ledgerId, entryId, length, buf));
                        buf.writeBytes(value.copyBytes());
                        entriesToRead--;
                        nextExpectedId++;
                    } else if (entryId > lastEntry) {
                        log.info("Expected to read {}, but read {}, which is greater than last entry {}",
                                nextExpectedId, entryId, lastEntry);
                        throw new BKException.BKUnexpectedConditionException();
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

    public static ReadHandle open(ScheduledExecutorService executor, MapFile.Reader reader, long ledgerId) throws IOException {
            return new FileStoreBackedReadHandleImpl(executor, reader, ledgerId);
    }

    private static LedgerMetadata parseLedgerMetadata(byte[] bytes) throws IOException {
        DataFormats.LedgerMetadataFormat ledgerMetadataFormat = DataFormats.LedgerMetadataFormat.newBuilder().mergeFrom(bytes).build();
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
                .withLastEntryId(ledgerMetadataFormat.getLastEntryId())
                .withPassword(ledgerMetadataFormat.getPassword().toByteArray())
                .withClosedState()
                .withMetadataFormatVersion(2)
                .withLength(ledgerMetadataFormat.getLength())
                .withAckQuorumSize(ledgerMetadataFormat.getAckQuorumSize())
                .withCreationTime(ledgerMetadataFormat.getCtime())
                .withWriteQuorumSize(ledgerMetadataFormat.getQuorumSize())
                .withEnsembleSize(ledgerMetadataFormat.getEnsembleSize());
        ledgerMetadataFormat.getSegmentList().forEach(segment -> {
            ArrayList<BookieSocketAddress> addressArrayList = new ArrayList<>();
            segment.getEnsembleMemberList().forEach(address -> {
                try {
                    addressArrayList.add(new BookieSocketAddress(address));
                } catch (IOException e) {
                    log.error("Exception when create BookieSocketAddress. ", e);
                }
            });
            builder.newEnsembleEntry(segment.getFirstEntryId(), addressArrayList);
        });

        if (ledgerMetadataFormat.getCustomMetadataCount() > 0) {
            Map<String, byte[]> customMetadata = Maps.newHashMap();
            ledgerMetadataFormat.getCustomMetadataList().forEach(
                    entry -> customMetadata.put(entry.getKey(), entry.getValue().toByteArray()));
            builder.withCustomMetadata(customMetadata);
        }

        switch (ledgerMetadataFormat.getDigestType()) {
            case HMAC:
                builder.withDigestType(DigestType.MAC);
                break;
            case CRC32:
                builder.withDigestType(DigestType.CRC32);
                break;
            case CRC32C:
                builder.withDigestType(DigestType.CRC32C);
                break;
            case DUMMY:
                builder.withDigestType(DigestType.DUMMY);
                break;
            default:
                throw new IllegalArgumentException("Unable to convert digest type " + ledgerMetadataFormat.getDigestType());
        }

        return builder.build();
    }

}
