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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;

import java.util.List;
import java.util.Map;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.BlockAwareSegmentInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jclouds.provider.factory.BlobStoreLocation;
import org.apache.bookkeeper.mledger.offload.jclouds.provider.factory.JCloudBlobStoreFactory;
import org.apache.bookkeeper.mledger.offload.jclouds.provider.factory.JCloudBlobStoreFactoryFactory;
import org.apache.bookkeeper.mledger.offload.jclouds.provider.factory.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jclouds.provider.factory.OffloadDriverMetadataKeys;
import org.apache.commons.lang3.tuple.Pair;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.jclouds.osgi.ProviderRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tiered Storage Offloader that is backed by a JCloud Blob Store.
 */
public class BlobStoreManagedLedgerOffloader implements LedgerOffloader, OffloadDriverMetadataKeys {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreManagedLedgerOffloader.class);

    private static void addVersionInfo(BlobBuilder blobBuilder, Map<String, String> userMetadata) {
        ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
        metadataBuilder.putAll(userMetadata);
        metadataBuilder.put(METADATA_FORMAT_VERSION_KEY.toLowerCase(), CURRENT_VERSION);
        blobBuilder.userMetadata(metadataBuilder.build());
    }

    private final OrderedScheduler scheduler;

    // container in jclouds to write offloaded ledgers
    private final String writeBucket;
    // the region to write offloaded ledgers
    private final String writeRegion;
    // the endpoint
    private final String writeEndpoint;
    // credentials
    private final Credentials credentials;

    // max block size for each data block.
    private int maxBlockSize;
    private final int readBufferSize;

    private final BlobStore writeBlobStore;
    private final Location writeLocation;

    private final ConcurrentMap<BlobStoreLocation, BlobStore> readBlobStores = new ConcurrentHashMap<>();

    // metadata to be stored as part of the offloaded ledger metadata
    private final Map<String, String> userMetadata;
    // offload driver metadata to be stored as part of the original ledger metadata

    private final JCloudBlobStoreProvider provider;

    private static Pair<BlobStoreLocation, BlobStore> createBlobStore(JCloudBlobStoreFactory factory) {
        ProviderRegistry.registerProvider(factory.getProviderMetadata());
        return Pair.of(factory.getBlobStoreLocation(), factory.getBlobStore());
    }

    public static BlobStoreManagedLedgerOffloader create(JCloudBlobStoreFactory factory,
            Map<String, String> userMetadata,
            OrderedScheduler scheduler) throws IOException {

        return new BlobStoreManagedLedgerOffloader(factory, scheduler, userMetadata);
    }

    BlobStoreManagedLedgerOffloader(JCloudBlobStoreFactory factory, OrderedScheduler scheduler,
                                    Map<String, String> userMetadata) {

        this.provider = factory.getProvider();
        this.scheduler = scheduler;
        this.readBufferSize = factory.getReadBufferSizeInBytes();
        this.writeBucket = factory.getBucket();
        this.writeRegion = factory.getRegion();
        this.writeEndpoint = factory.getServiceEndpoint();
        this.maxBlockSize = factory.getMaxBlockSizeInBytes();
        this.userMetadata = userMetadata;
        this.credentials = factory.getCredentials();

        if (!Strings.isNullOrEmpty(writeRegion)) {
            this.writeLocation = new LocationBuilder()
                .scope(LocationScope.REGION)
                .id(writeRegion)
                .description(writeRegion)
                .build();
        } else {
            this.writeLocation = null;
        }

        log.info("Constructor offload driver: {}, host: {}, container: {}, region: {} ",
                factory.getProvider().getDriver(), factory.getServiceEndpoint(),
                factory.getBucket(), factory.getRegion());

        Pair<BlobStoreLocation, BlobStore> pair = createBlobStore(factory);
        this.writeBlobStore = pair.getRight();
        this.readBlobStores.put(pair.getLeft(), pair.getRight());
    }

    // build context for jclouds BlobStoreContext, Only used in test
    @VisibleForTesting
    BlobStoreManagedLedgerOffloader(BlobStore blobStore, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize) {
        this(blobStore, container, scheduler, maxBlockSize, readBufferSize, Maps.newHashMap());
    }

    BlobStoreManagedLedgerOffloader(BlobStore blobStore, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize,
                                    Map<String, String> userMetadata) {
        // TODO Fix this
        this.provider = JCloudBlobStoreProvider.AZURE_BLOB;
        this.scheduler = scheduler;
        this.readBufferSize = readBufferSize;
        this.writeBucket = container;
        this.writeRegion = null;
        this.writeEndpoint = null;
        this.maxBlockSize = maxBlockSize;
        this.writeBlobStore = blobStore;
        this.writeLocation = null;
        this.userMetadata = userMetadata;
        this.credentials = null;

        readBlobStores.put(
            BlobStoreLocation.of(provider.name(), writeRegion, container, writeEndpoint),
            blobStore
        );
    }

    static String dataBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d", uuid.toString(), ledgerId);
    }

    static String indexBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d-index", uuid.toString(), ledgerId);
    }

    public boolean createBucket(String bucket) {
        return writeBlobStore.createContainerInLocation(writeLocation, bucket);
    }

    public void deleteBucket(String bucket) {
        writeBlobStore.deleteContainer(bucket);
    }

    @Override
    public String getOffloadDriverName() {
        return provider.getDriver();
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        return ImmutableMap.of(
            METADATA_FIELD_BLOB_STORE_PROVIDER, provider.name(),
            METADATA_FIELD_BUCKET, writeBucket,
            METADATA_FIELD_REGION, writeRegion,
            METADATA_FIELD_ENDPOINT, writeEndpoint
        );
    }

    // upload DataBlock to s3 using MultiPartUpload, and indexBlock in a new Block,
    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle,
                                           UUID uuid,
                                           Map<String, String> extraMetadata) {
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
            String dataBlockKey = dataBlockOffloadKey(readHandle.getId(), uuid);
            String indexBlockKey = indexBlockOffloadKey(readHandle.getId(), uuid);

            MultipartUpload mpu = null;
            List<MultipartPart> parts = Lists.newArrayList();

            // init multi part upload for data block.
            try {
                BlobBuilder blobBuilder = writeBlobStore.blobBuilder(dataBlockKey);
                addVersionInfo(blobBuilder, userMetadata);
                Blob blob = blobBuilder.build();
                mpu = writeBlobStore.initiateMultipartUpload(writeBucket, blob.getMetadata(), new PutOptions());
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
                        .calculateBlockSize(maxBlockSize, readHandle, startEntry, entryBytesWritten);

                    try (BlockAwareSegmentInputStream blockStream = new BlockAwareSegmentInputStreamImpl(
                        readHandle, startEntry, blockSize)) {

                        Payload partPayload = Payloads.newInputStreamPayload(blockStream);
                        partPayload.getContentMetadata().setContentLength((long) blockSize);
                        partPayload.getContentMetadata().setContentType("application/octet-stream");
                        parts.add(writeBlobStore.uploadMultipartPart(mpu, partId, partPayload));
                        log.debug("UploadMultipartPart. container: {}, blobName: {}, partId: {}, mpu: {}",
                            writeBucket, dataBlockKey, partId, mpu.id());

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
                        writeBucket, dataBlockKey, mpu.id(), throwable);
                }
                promise.completeExceptionally(t);
                return;
            }

            // upload index block
            try (OffloadIndexBlock index = indexBuilder.withDataObjectLength(dataObjectLength).build();
                 OffloadIndexBlock.IndexInputStream indexStream = index.toStream()) {
                // write the index block
                BlobBuilder blobBuilder = writeBlobStore.blobBuilder(indexBlockKey);
                addVersionInfo(blobBuilder, userMetadata);
                Payload indexPayload = Payloads.newInputStreamPayload(indexStream);
                indexPayload.getContentMetadata().setContentLength((long) indexStream.getStreamSize());
                indexPayload.getContentMetadata().setContentType("application/octet-stream");

                Blob blob = blobBuilder
                    .payload(indexPayload)
                    .contentLength((long) indexStream.getStreamSize())
                    .build();

                writeBlobStore.putBlob(writeBucket, blob);
                promise.complete(null);
            } catch (Throwable t) {
                try {
                    writeBlobStore.removeBlob(writeBucket, dataBlockKey);
                } catch (Throwable throwable) {
                    log.error("Failed deleteObject in bucket - {} with key - {}.",
                        writeBucket, dataBlockKey, throwable);
                }
                promise.completeExceptionally(t);
                return;
            }
        });
        return promise;
    }

    String getReadProvider(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(METADATA_FIELD_BLOB_STORE_PROVIDER, "AZURE_BLOB");
    }

    String getReadRegion(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(METADATA_FIELD_REGION, writeRegion);
    }

    String getReadBucket(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(METADATA_FIELD_BUCKET, writeBucket);
    }

    String getReadEndpoint(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(METADATA_FIELD_ENDPOINT, writeEndpoint);
    }

    BlobStore getReadBlobStore(Map<String, String> offloadDriverMetadata) {

        BlobStoreLocation location = BlobStoreLocation.of(
            getReadProvider(offloadDriverMetadata),
            getReadRegion(offloadDriverMetadata),
            getReadBucket(offloadDriverMetadata),
            getReadEndpoint(offloadDriverMetadata)
        );

        BlobStore blobStore = readBlobStores.get(location);
        if (null == blobStore) {

            try {
                JCloudBlobStoreFactory factory = JCloudBlobStoreFactoryFactory.create(offloadDriverMetadata);
                factory.setRegion(location.getRegion());
                factory.setServiceEndpoint(location.getEndpoint());
                factory.setCredentials(credentials);
                factory.setMaxBlockSizeInBytes(maxBlockSize);

                blobStore = createBlobStore(factory).getRight();
            } catch (final IOException ioEx) {
                log.error("Failed getReadBlobStore: ", ioEx);
                return null;
            }

            BlobStore existingBlobStore = readBlobStores.putIfAbsent(location, blobStore);

            if (null == existingBlobStore) {
                return blobStore;
            } else {
                return existingBlobStore;
            }
        } else {
            return blobStore;
        }
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                       Map<String, String> offloadDriverMetadata) {
        String readBucket = getReadBucket(offloadDriverMetadata);
        BlobStore readBlobstore = getReadBlobStore(offloadDriverMetadata);

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String key = dataBlockOffloadKey(ledgerId, uid);
        String indexKey = indexBlockOffloadKey(ledgerId, uid);
        scheduler.chooseThread(ledgerId).submit(() -> {
                try {
                    promise.complete(BlobStoreBackedReadHandleImpl.open(scheduler.chooseThread(ledgerId),
                                                                 readBlobstore,
                                                                 readBucket, key, indexKey,
                                                                 VERSION_CHECK,
                                                                 ledgerId, readBufferSize));
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
        String readBucket = getReadBucket(offloadDriverMetadata);
        BlobStore readBlobstore = getReadBlobStore(offloadDriverMetadata);

        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                readBlobstore.removeBlobs(readBucket,
                    ImmutableList.of(dataBlockOffloadKey(ledgerId, uid), indexBlockOffloadKey(ledgerId, uid)));
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete Blob", t);
                promise.completeExceptionally(t);
            }
        });

        return promise;
    }

    /**
     * Version checking marker interface.
     */
    public interface VersionCheck {
        void check(String key, Blob blob) throws IOException;
    }

    public BlobStore getWriteBlobStore() {
        return writeBlobStore;
    }
}