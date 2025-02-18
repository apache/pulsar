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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class BookkeeperBucketSnapshotStorage implements BucketSnapshotStorage {

    private static final byte[] LedgerPassword = "".getBytes();

    private final PulsarService pulsar;
    private final ServiceConfiguration config;
    private BookKeeper bookKeeper;

    private final Map<Long, CompletableFuture<LedgerHandle>> ledgerHandleFutureCache = new ConcurrentHashMap<>();

    public BookkeeperBucketSnapshotStorage(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.config = pulsar.getConfig();
    }

    @Override
    public CompletableFuture<Long> createBucketSnapshot(SnapshotMetadata snapshotMetadata,
                                                        List<SnapshotSegment> bucketSnapshotSegments,
                                                        String bucketKey, String topicName, String cursorName) {
        ByteBuf metadataByteBuf = Unpooled.wrappedBuffer(snapshotMetadata.toByteArray());
        return createLedger(bucketKey, topicName, cursorName)
                .thenCompose(ledgerHandle -> addEntry(ledgerHandle, metadataByteBuf)
                        .thenCompose(__ -> addSnapshotSegments(ledgerHandle, bucketSnapshotSegments))
                        .thenCompose(__ -> closeLedger(ledgerHandle))
                        .thenApply(__ -> ledgerHandle.getId()));
    }

    @Override
    public CompletableFuture<SnapshotMetadata> getBucketSnapshotMetadata(long bucketId) {
        return getLedgerHandle(bucketId).thenCompose(ledgerHandle -> getLedgerEntry(ledgerHandle, 0, 0)
                .thenApply(entryEnumeration -> parseSnapshotMetadataEntry(entryEnumeration.nextElement())));
    }

    @Override
    public CompletableFuture<List<SnapshotSegment>> getBucketSnapshotSegment(long bucketId, long firstSegmentEntryId,
                                                                             long lastSegmentEntryId) {
        return getLedgerHandle(bucketId).thenCompose(
                ledgerHandle -> getLedgerEntry(ledgerHandle, firstSegmentEntryId, lastSegmentEntryId)
                        .thenApply(this::parseSnapshotSegmentEntries));
    }

    @Override
    public CompletableFuture<Long> getBucketSnapshotLength(long bucketId) {
        return getLedgerHandle(bucketId).thenCompose(
                ledgerHandle -> CompletableFuture.completedFuture(ledgerHandle.getLength()));
    }

    @Override
    public CompletableFuture<Void> deleteBucketSnapshot(long bucketId) {
        CompletableFuture<LedgerHandle> ledgerHandleFuture = ledgerHandleFutureCache.remove(bucketId);
        if (ledgerHandleFuture != null) {
            ledgerHandleFuture.whenComplete((lh, ex) -> closeLedger(lh));
        }
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
        ).get();
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
        ByteBuf byteBuf;
        for (SnapshotSegment bucketSnapshotSegment : bucketSnapshotSegments) {
            byteBuf = PulsarByteBufAllocator.DEFAULT.directBuffer(bucketSnapshotSegment.getSerializedSize());
            try {
                bucketSnapshotSegment.writeTo(byteBuf);
            } catch (Exception e){
                byteBuf.release();
                throw e;
            }
            addFutures.add(addEntry(ledgerHandle, byteBuf));
        }

        return FutureUtil.waitForAll(addFutures);
    }

    private SnapshotMetadata parseSnapshotMetadataEntry(LedgerEntry ledgerEntry) {
        ByteBuf entryBuffer = null;
        try {
            entryBuffer = ledgerEntry.getEntryBuffer();
            return SnapshotMetadata.parseFrom(entryBuffer.nioBuffer());
        } catch (InvalidProtocolBufferException e) {
            throw new BucketSnapshotSerializationException(e);
        } finally {
            if (entryBuffer != null) {
                entryBuffer.release();
            }
        }
    }

    private List<SnapshotSegment> parseSnapshotSegmentEntries(Enumeration<LedgerEntry> entryEnumeration) {
        List<SnapshotSegment> snapshotMetadataList = new ArrayList<>();
        while (entryEnumeration.hasMoreElements()) {
            LedgerEntry ledgerEntry = entryEnumeration.nextElement();
            SnapshotSegment snapshotSegment = new SnapshotSegment();
            ByteBuf entryBuffer = ledgerEntry.getEntryBuffer();
            try {
                snapshotSegment.parseFrom(entryBuffer, entryBuffer.readableBytes());
            } finally {
                entryBuffer.release();
            }
            snapshotMetadataList.add(snapshotSegment);
        }
        return snapshotMetadataList;
    }

    @NotNull
    private CompletableFuture<LedgerHandle> createLedger(String bucketKey, String topicName, String cursorName) {
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        Map<String, byte[]> metadata = LedgerMetadataUtils.buildMetadataForDelayedIndexBucket(bucketKey,
                topicName, cursorName);
        bookKeeper.asyncCreateLedger(
                config.getManagedLedgerDefaultEnsembleSize(),
                config.getManagedLedgerDefaultWriteQuorum(),
                config.getManagedLedgerDefaultAckQuorum(),
                BookKeeper.DigestType.fromApiDigestType(config.getManagedLedgerDigestType()),
                LedgerPassword,
                (rc, handle, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Create ledger", rc, -1));
                    } else {
                        future.complete(handle);
                    }
                }, null, metadata);
        return future;
    }

    private CompletableFuture<LedgerHandle> getLedgerHandle(Long ledgerId) {
        CompletableFuture<LedgerHandle> ledgerHandleCompletableFuture =
                ledgerHandleFutureCache.computeIfAbsent(ledgerId, k -> openLedger(ledgerId));
        // remove future of completed exceptionally
        ledgerHandleCompletableFuture.whenComplete((__, ex) -> {
            if (ex != null) {
                ledgerHandleFutureCache.remove(ledgerId, ledgerHandleCompletableFuture);
            }
        });
        return ledgerHandleCompletableFuture;
    }

    private CompletableFuture<LedgerHandle> openLedger(Long ledgerId) {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        bookKeeper.asyncOpenLedger(
                ledgerId,
                BookKeeper.DigestType.fromApiDigestType(config.getManagedLedgerDigestType()),
                LedgerPassword,
                (rc, handle, ctx) -> {
                    if (rc == BKException.Code.NoSuchLedgerExistsException) {
                        // If the ledger does not exist, throw BucketNotExistException
                        future.completeExceptionally(noSuchLedgerException("Open ledger", ledgerId));
                    } else if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Open ledger", rc, ledgerId));
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
                future.completeExceptionally(bkException("Close ledger", rc, ledgerHandle.getId()));
            } else {
                future.complete(null);
            }
        }, null);
        return future;
    }

    private CompletableFuture<Void> addEntry(LedgerHandle ledgerHandle, ByteBuf data) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        ledgerHandle.asyncAddEntry(data,
                (rc, handle, entryId, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Add entry", rc, ledgerHandle.getId()));
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

    CompletableFuture<Enumeration<LedgerEntry>> getLedgerEntry(LedgerHandle ledger,
                                                               long firstEntryId, long lastEntryId) {
        final CompletableFuture<Enumeration<LedgerEntry>> future = new CompletableFuture<>();
        ledger.asyncReadEntries(firstEntryId, lastEntryId,
                (rc, handle, entries, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Read entry", rc, ledger.getId()));
                    } else {
                        future.complete(entries);
                    }
                }, null
        );
        return future;
    }

    private CompletableFuture<Void> deleteLedger(long ledgerId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        bookKeeper.asyncDeleteLedger(ledgerId, (int rc, Object cnx) -> {
            if (rc == BKException.Code.NoSuchLedgerExistsException || rc == BKException.Code.OK) {
                // If the ledger does not exist or has been deleted, we can treat it as success
                future.complete(null);
            } else  {
                future.completeExceptionally(bkException("Delete ledger", rc, ledgerId));
            }
        }, null);
        return future;
    }

    private static BucketSnapshotPersistenceException bkException(String operation, int rc, long ledgerId) {
        String message = BKException.getMessage(rc)
                + " -  ledger=" + ledgerId + " - operation=" + operation;
        return new BucketSnapshotPersistenceException(message);
    }

    private static BucketNotExistException noSuchLedgerException(String operation, long ledgerId) {
        String message = BKException.getMessage(BKException.Code.NoSuchLedgerExistsException)
                + " - ledger=" + ledgerId + " - operation=" + operation;
        return new BucketNotExistException(message);
    }
}
