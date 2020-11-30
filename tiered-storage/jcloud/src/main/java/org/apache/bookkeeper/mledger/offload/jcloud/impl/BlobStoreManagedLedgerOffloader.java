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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.BlockAwareSegmentInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.BlobStoreLocation;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;

/**
 * Tiered Storage Offloader that is backed by a JCloud Blob Store.
 * <p>
 * The constructor takes an instance of TieredStorageConfiguration, which
 * contains all of the configuration data necessary to connect to a JCloud
 * Provider service.
 * </p>
 */
@Slf4j
public class BlobStoreManagedLedgerOffloader implements LedgerOffloader {

    private final OrderedScheduler scheduler;
    private final TieredStorageConfiguration config;
    private final Location writeLocation;

    // metadata to be stored as part of the offloaded ledger metadata
    private final Map<String, String> userMetadata;

    private final ConcurrentMap<BlobStoreLocation, BlobStore> blobStores = new ConcurrentHashMap<>();

    public static BlobStoreManagedLedgerOffloader create(TieredStorageConfiguration config,
            Map<String, String> userMetadata,
            OrderedScheduler scheduler) throws IOException {

        return new BlobStoreManagedLedgerOffloader(config, scheduler, userMetadata);
    }

    BlobStoreManagedLedgerOffloader(TieredStorageConfiguration config, OrderedScheduler scheduler,
                                    Map<String, String> userMetadata) {

        this.scheduler = scheduler;
        this.userMetadata = userMetadata;
        this.config = config;

        if (!Strings.isNullOrEmpty(config.getRegion())) {
            this.writeLocation = new LocationBuilder()
                .scope(LocationScope.REGION)
                .id(config.getRegion())
                .description(config.getRegion())
                .build();
        } else {
            this.writeLocation = null;
        }

        log.info("Constructor offload driver: {}, host: {}, container: {}, region: {} ",
                config.getProvider().getDriver(), config.getServiceEndpoint(),
                config.getBucket(), config.getRegion());

        blobStores.putIfAbsent(config.getBlobStoreLocation(), config.getBlobStore());
        log.info("The ledger offloader was created.");
    }

    @Override
    public String getOffloadDriverName() {
        return config.getDriver();
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        return config.getOffloadDriverMetadata();
    }

    /**
     * Upload the DataBlocks associated with the given ReadHandle using MultiPartUpload,
     * Creating indexBlocks for each corresponding DataBlock that is uploaded.
     */
    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle,
                                           UUID uuid,
                                           Map<String, String> extraMetadata) {
        final BlobStore writeBlobStore = blobStores.get(config.getBlobStoreLocation());
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).submit(() -> {
            if (readHandle.getLength() == 0 || !readHandle.isClosed() || readHandle.getLastAddConfirmed() < 0) {
                promise.completeExceptionally(
                        new IllegalArgumentException("An empty or open ledger should never be offloaded"));
                return;
            }
            OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create()
                .withLedgerMetadata(readHandle.getLedgerMetadata())
                .withDataBlockHeaderLength(BlockAwareSegmentInputStreamImpl.getHeaderSize());
            String dataBlockKey = DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid);
            String indexBlockKey = DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid);

            MultipartUpload mpu = null;
            List<MultipartPart> parts = Lists.newArrayList();

            // init multi part upload for data block.
            try {
                BlobBuilder blobBuilder = writeBlobStore.blobBuilder(dataBlockKey);
                DataBlockUtils.addVersionInfo(blobBuilder, userMetadata);
                Blob blob = blobBuilder.build();
                mpu = writeBlobStore.initiateMultipartUpload(config.getBucket(), blob.getMetadata(), new PutOptions());
            } catch (Throwable t) {
                promise.completeExceptionally(t);
                return;
            }

            long dataObjectLength = 0;
            // start multi part upload for data block.
            try {
                long startEntry = 0;
                int partId = 1;
                long entryBytesWritten = 0;
                while (startEntry <= readHandle.getLastAddConfirmed()) {
                    int blockSize = BlockAwareSegmentInputStreamImpl
                        .calculateBlockSize(config.getMaxBlockSizeInBytes(), readHandle, startEntry, entryBytesWritten);

                    try (BlockAwareSegmentInputStream blockStream = new BlockAwareSegmentInputStreamImpl(
                        readHandle, startEntry, blockSize)) {

                        Payload partPayload = Payloads.newInputStreamPayload(blockStream);
                        partPayload.getContentMetadata().setContentLength((long) blockSize);
                        partPayload.getContentMetadata().setContentType("application/octet-stream");
                        parts.add(writeBlobStore.uploadMultipartPart(mpu, partId, partPayload));
                        log.debug("UploadMultipartPart. container: {}, blobName: {}, partId: {}, mpu: {}",
                                config.getBucket(), dataBlockKey, partId, mpu.id());

                        indexBuilder.addBlock(startEntry, partId, blockSize);

                        if (blockStream.getEndEntryId() != -1) {
                            startEntry = blockStream.getEndEntryId() + 1;
                        } else {
                            // could not read entry from ledger.
                            break;
                        }
                        entryBytesWritten += blockStream.getBlockEntryBytesCount();
                        partId++;
                    }

                    dataObjectLength += blockSize;
                }

                writeBlobStore.completeMultipartUpload(mpu, parts);
                mpu = null;
            } catch (Throwable t) {
                try {
                    if (mpu != null) {
                        writeBlobStore.abortMultipartUpload(mpu);
                    }
                } catch (Throwable throwable) {
                    log.error("Failed abortMultipartUpload in bucket - {} with key - {}, uploadId - {}.",
                            config.getBucket(), dataBlockKey, mpu.id(), throwable);
                }
                promise.completeExceptionally(t);
                return;
            }

            // upload index block
            try (OffloadIndexBlock index = indexBuilder.withDataObjectLength(dataObjectLength).build();
                 OffloadIndexBlock.IndexInputStream indexStream = index.toStream()) {
                // write the index block
                BlobBuilder blobBuilder = writeBlobStore.blobBuilder(indexBlockKey);
                DataBlockUtils.addVersionInfo(blobBuilder, userMetadata);
                Payload indexPayload = Payloads.newInputStreamPayload(indexStream);
                indexPayload.getContentMetadata().setContentLength((long) indexStream.getStreamSize());
                indexPayload.getContentMetadata().setContentType("application/octet-stream");

                Blob blob = blobBuilder
                    .payload(indexPayload)
                    .contentLength((long) indexStream.getStreamSize())
                    .build();

                writeBlobStore.putBlob(config.getBucket(), blob);
                promise.complete(null);
            } catch (Throwable t) {
                try {
                    writeBlobStore.removeBlob(config.getBucket(), dataBlockKey);
                } catch (Throwable throwable) {
                    log.error("Failed deleteObject in bucket - {} with key - {}.",
                            config.getBucket(), dataBlockKey, throwable);
                }
                promise.completeExceptionally(t);
                return;
            }
        });
        return promise;
    }

    /**
     * Attempts to create a BlobStoreLocation from the values in the offloadDriverMetadata,
     * however, if no values are available, it defaults to the currently configured
     * provider, region, bucket, etc.
     *
     * @param offloadDriverMetadata
     * @return
     */
    private BlobStoreLocation getBlobStoreLocation(Map<String, String> offloadDriverMetadata) {
        return (!offloadDriverMetadata.isEmpty()) ? new BlobStoreLocation(offloadDriverMetadata) :
            new BlobStoreLocation(getOffloadDriverMetadata());
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                       Map<String, String> offloadDriverMetadata) {

        BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        String readBucket = bsKey.getBucket();
        BlobStore readBlobstore = blobStores.get(config.getBlobStoreLocation());

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String key = DataBlockUtils.dataBlockOffloadKey(ledgerId, uid);
        String indexKey = DataBlockUtils.indexBlockOffloadKey(ledgerId, uid);
        scheduler.chooseThread(ledgerId).submit(() -> {
                try {
                    promise.complete(BlobStoreBackedReadHandleImpl.open(scheduler.chooseThread(ledgerId),
                                                                 readBlobstore,
                                                                 readBucket, key, indexKey,
                                                                 DataBlockUtils.VERSION_CHECK,
                                                                 ledgerId, config.getReadBufferSizeInBytes()));
                } catch (Throwable t) {
                    log.error("Failed readOffloaded: ", t);
                    promise.completeExceptionally(t);
                }
            });
        return promise;
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid,
                                                   Map<String, String> offloadDriverMetadata) {
        BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        String readBucket = bsKey.getBucket(offloadDriverMetadata);
        BlobStore readBlobstore = blobStores.get(config.getBlobStoreLocation());

        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                readBlobstore.removeBlobs(readBucket,
                    ImmutableList.of(DataBlockUtils.dataBlockOffloadKey(ledgerId, uid),
                                     DataBlockUtils.indexBlockOffloadKey(ledgerId, uid)));
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete Blob", t);
                promise.completeExceptionally(t);
            }
        });

        return promise;
    }

    
    @Override
    public OffloadPolicies getOffloadPolicies() {
        Properties properties = new Properties();
        properties.putAll(config.getConfigProperties());
        return OffloadPolicies.create(properties);
    }

    @Override
    public void close() {
        for (BlobStore readBlobStore : blobStores.values()) {
            if (readBlobStore != null) {
                readBlobStore.getContext().close();
            }
        }
    }
}