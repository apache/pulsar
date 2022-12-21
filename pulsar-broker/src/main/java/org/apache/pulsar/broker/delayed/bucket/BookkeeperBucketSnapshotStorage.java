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

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class BookkeeperBucketSnapshotStorage implements BucketSnapshotStorage {

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
                                                        List<SnapshotSegment> bucketSnapshotSegments,
                                                        String bucketKey) {
        return createLedger(bucketKey)
                .thenCompose(ledgerHandle -> addEntry(ledgerHandle, snapshotMetadata.toByteArray())
                        .thenCompose(__ -> addSnapshotSegments(ledgerHandle, bucketSnapshotSegments))
                        .thenCompose(__ -> closeLedger(ledgerHandle))
                        .thenApply(__ -> ledgerHandle.getId()));
    }

    @Override
    public CompletableFuture<SnapshotMetadata> getBucketSnapshotMetadata(long bucketId) {
        return openLedger(bucketId).thenCompose(
                ledgerHandle -> getLedgerEntryThenCloseLedger(ledgerHandle, 0, 0).
                        thenApply(entryEnumeration -> parseSnapshotMetadataEntry(entryEnumeration.nextElement())));
    }

    @Override
    public CompletableFuture<List<SnapshotSegment>> getBucketSnapshotSegment(long bucketId, long firstSegmentEntryId,
                                                                             long lastSegmentEntryId) {
        return openLedger(bucketId).thenCompose(
                ledgerHandle -> getLedgerEntryThenCloseLedger(ledgerHandle, firstSegmentEntryId,
                        lastSegmentEntryId).thenApply(this::parseSnapshotSegmentEntries));
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

    private SnapshotMetadata parseSnapshotMetadataEntry(LedgerEntry ledgerEntry) {
        try {
            return SnapshotMetadata.parseFrom(ledgerEntry.getEntry());
        } catch (InvalidProtocolBufferException e) {
            throw new BucketSnapshotSerializationException(e);
        }
    }

    private List<SnapshotSegment> parseSnapshotSegmentEntries(Enumeration<LedgerEntry> entryEnumeration) {
        List<SnapshotSegment> snapshotMetadataList = new ArrayList<>();
        try {
            while (entryEnumeration.hasMoreElements()) {
                LedgerEntry ledgerEntry = entryEnumeration.nextElement();
                snapshotMetadataList.add(SnapshotSegment.parseFrom(ledgerEntry.getEntry()));
            }
            return snapshotMetadataList;
        } catch (IOException e) {
            throw new BucketSnapshotSerializationException(e);
        }
    }

    @NotNull
    private CompletableFuture<LedgerHandle> createLedger(String bucketKey) {
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        Map<String, byte[]> metadata = LedgerMetadataUtils.buildMetadataForDelayedIndexBucket(bucketKey);
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
                }, null, metadata);
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

        return future.whenComplete((__, ex) -> {
            if (ex != null) {
                deleteLedger(ledgerHandle.getId());
            }
        });
    }

    CompletableFuture<Enumeration<LedgerEntry>> getLedgerEntryThenCloseLedger(LedgerHandle ledger,
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

    private static BucketSnapshotPersistenceException bkException(String operation, int rc, long ledgerId) {
        String message = BKException.getMessage(rc)
                + " -  ledger=" + ledgerId + " - operation=" + operation;
        return new BucketSnapshotPersistenceException(message);
    }
}
