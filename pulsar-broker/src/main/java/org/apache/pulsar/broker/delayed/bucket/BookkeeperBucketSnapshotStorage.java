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
package org.apache.pulsar.broker.delayed.bucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.validation.constraints.NotNull;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookkeeperBucketSnapshotStorage implements BucketSnapshotStorage {

    private static final Logger log = LoggerFactory.getLogger(BookkeeperBucketSnapshotStorage.class);
    private static final byte[] LedgerPassword = "".getBytes();

    private final PulsarService pulsar;
    private final ServiceConfiguration config;
    private BookKeeper bookKeeper;

    public BookkeeperBucketSnapshotStorage(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.config = pulsar.getConfig();
    }

    @Override
    public CompletableFuture<Long> createBucketSnapshot(SnapshotMetadata snapshotMetadata,
                                                        List<SnapshotSegment> bucketSnapshotSegments) {
        return createLedger()
                .thenCompose(ledgerHandle -> addEntry(ledgerHandle, snapshotMetadata.toByteArray())
                                .thenApply(__ -> ledgerHandle))
                .thenCompose(ledgerHandle -> addSnapshotSegments(ledgerHandle, bucketSnapshotSegments)
                        .thenApply(__ -> ledgerHandle))
                .thenCompose(ledgerHandle -> closeLedger(ledgerHandle)
                        .thenApply(__ -> ledgerHandle.getId()));
    }

    @Override
    public CompletableFuture<SnapshotMetadata> getBucketSnapshotMetadata(long bucketId) {
        return openLedger(bucketId).thenCompose(
                ledgerHandle -> getLedgerEntry(ledgerHandle, 0, 0).
                        thenCompose(
                        entryEnumeration -> parseSnapshotMetadataEntry(entryEnumeration.nextElement())));
    }

    @Override
    public CompletableFuture<List<SnapshotSegment>> getBucketSnapshotSegment(long bucketId, long firstSegmentEntryId,
                                                                             long lastSegmentEntryId) {
        return openLedger(bucketId).thenCompose(
                ledgerHandle -> getLedgerEntry(ledgerHandle, firstSegmentEntryId, lastSegmentEntryId).thenCompose(
                        this::parseSnapshotSegmentEntries));
    }

    @Override
    public CompletableFuture<Long> getBucketSnapshotLength(long bucketId) {
        return openLedger(bucketId).thenApply(ledgerHandle -> {
            long length = ledgerHandle.getLength();
            closeLedger(ledgerHandle);
            return length;
        });
    }

    @Override
    public CompletableFuture<Void> deleteBucketSnapshot(long bucketId) {
        return deleteLedger(bucketId);
    }

    @Override
    public void start() throws Exception {
        this.bookKeeper = pulsar.getBookKeeperClientFactory().create(
                pulsar.getConfiguration(),
                pulsar.getLocalMetadataStore(),
                pulsar.getIoEventLoopGroup(),
                Optional.empty(),
                null
        );
    }

    @Override
    public void close() throws Exception {
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    private CompletableFuture<Void> addSnapshotSegments(LedgerHandle ledgerHandle,
                                                        List<SnapshotSegment> bucketSnapshotSegments) {
        List<CompletableFuture<Void>> addFutures = new ArrayList<>();
        for (SnapshotSegment bucketSnapshotSegment : bucketSnapshotSegments) {
            addFutures.add(addEntry(ledgerHandle, bucketSnapshotSegment.toByteArray()));
        }

        return FutureUtil.waitForAll(addFutures);
    }

    private CompletableFuture<SnapshotMetadata> parseSnapshotMetadataEntry(LedgerEntry ledgerEntry) {
        CompletableFuture<SnapshotMetadata> result = new CompletableFuture<>();
        try {
            result.complete(SnapshotMetadata.parseFrom(ledgerEntry.getEntry()));
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    private CompletableFuture<List<SnapshotSegment>> parseSnapshotSegmentEntries(
            Enumeration<LedgerEntry> entryEnumeration) {
        CompletableFuture<List<SnapshotSegment>> result = new CompletableFuture<>();
        List<SnapshotSegment> snapshotMetadataList = new ArrayList<>();
        try {
            while (entryEnumeration.hasMoreElements()) {
                LedgerEntry ledgerEntry = entryEnumeration.nextElement();
                snapshotMetadataList.add(SnapshotSegment.parseFrom(ledgerEntry.getEntry()));
            }
            result.complete(snapshotMetadataList);
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @NotNull
    private CompletableFuture<LedgerHandle> createLedger() {
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        bookKeeper.asyncCreateLedger(
                config.getManagedLedgerDefaultEnsembleSize(),
                config.getManagedLedgerDefaultWriteQuorum(),
                config.getManagedLedgerDefaultAckQuorum(),
                BookKeeper.DigestType.fromApiDigestType(config.getManagedLedgerDigestType()),
                LedgerPassword,
                (rc, handle, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Failed to create ledger", rc, -1));
                    } else {
                        future.complete(handle);
                    }
                }, null, null);
        return future;
    }

    private CompletableFuture<LedgerHandle> openLedger(Long ledgerId) {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        bookKeeper.asyncOpenLedger(
                ledgerId,
                BookKeeper.DigestType.fromApiDigestType(config.getManagedLedgerDigestType()),
                LedgerPassword,
                (rc, handle, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Failed to open ledger", rc, ledgerId));
                    } else {
                        future.complete(handle);
                    }
                }, null
        );
        return future;
    }

    private CompletableFuture<Void> closeLedger(LedgerHandle ledgerHandle) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ledgerHandle.asyncClose((rc, handle, ctx) -> {
            if (rc != BKException.Code.OK) {
                log.warn("Failed to close a Ledger Handle: {}", ledgerHandle.getId());
                future.completeExceptionally(bkException("Failed to close ledger", rc, ledgerHandle.getId()));
            } else {
                future.complete(null);
            }
        }, null);
        return future;
    }

    private CompletableFuture<Void> addEntry(LedgerHandle ledgerHandle, byte[] data) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        ledgerHandle.asyncAddEntry(data,
                (rc, handle, entryId, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Failed to add entry", rc, ledgerHandle.getId()));
                    } else {
                        future.complete(null);
                    }
                }, null
        );
        return future;
    }

    CompletableFuture<Enumeration<LedgerEntry>> getLedgerEntry(LedgerHandle ledger,
                                                               long firstEntryId, long lastEntryId) {
        final CompletableFuture<Enumeration<LedgerEntry>> future = new CompletableFuture<>();
        ledger.asyncReadEntries(firstEntryId, lastEntryId,
                (rc, handle, entries, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Failed to read entry", rc, ledger.getId()));
                    } else {
                        future.complete(entries);
                    }
                    closeLedger(handle);
                }, null
        );
        return future;
    }

    private CompletableFuture<Void> deleteLedger(long ledgerId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        bookKeeper.asyncDeleteLedger(ledgerId, (int rc, Object cnx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(bkException("Failed to delete ledger", rc, ledgerId));
            } else {
                future.complete(null);
            }
        }, null);
        return future;
    }

    private static BucketSnapshotException bkException(String operation, int rc, long ledgerId) {
        String message = BKException.getMessage(rc)
                + " -  ledger=" + ledgerId + " - operation=" + operation;
        return new BucketSnapshotException(message);
    }
}
