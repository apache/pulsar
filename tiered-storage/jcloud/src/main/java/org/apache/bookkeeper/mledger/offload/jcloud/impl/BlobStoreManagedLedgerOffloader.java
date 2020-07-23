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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Data;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.BlockAwareSegmentInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.CredentialsUtil;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.jcloud.shade.com.google.common.base.Supplier;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.domain.SessionCredentials;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.jclouds.googlecloudstorage.GoogleCloudStorageProviderMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.jclouds.osgi.ApiRegistry;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.s3.S3ApiMetadata;
import org.jclouds.s3.reference.S3Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStoreManagedLedgerOffloader implements LedgerOffloader {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreManagedLedgerOffloader.class);

    private static final String METADATA_FIELD_BUCKET = "bucket";
    private static final String METADATA_FIELD_REGION = "region";
    private static final String METADATA_FIELD_ENDPOINT = "endpoint";

    public static final String[] DRIVER_NAMES = {"S3", "aws-s3", "google-cloud-storage"};

    // use these keys for both s3 and gcs.
    static final String METADATA_FORMAT_VERSION_KEY = "S3ManagedLedgerOffloaderFormatVersion";
    static final String CURRENT_VERSION = String.valueOf(1);

    public static boolean driverSupported(String driver) {
        return Arrays.stream(DRIVER_NAMES).anyMatch(d -> d.equalsIgnoreCase(driver));
    }

    public static boolean isS3Driver(String driver) {
        return driver.equalsIgnoreCase(DRIVER_NAMES[0]) || driver.equalsIgnoreCase(DRIVER_NAMES[1]);
    }

    public static boolean isGcsDriver(String driver) {
        return driver.equalsIgnoreCase(DRIVER_NAMES[2]);
    }

    private static void addVersionInfo(BlobBuilder blobBuilder, Map<String, String> userMetadata) {
        ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
        metadataBuilder.putAll(userMetadata);
        metadataBuilder.put(METADATA_FORMAT_VERSION_KEY.toLowerCase(), CURRENT_VERSION);
        blobBuilder.userMetadata(metadataBuilder.build());
    }

    @Data(staticConstructor = "of")
    private static class BlobStoreLocation {
        private final String region;
        private final String endpoint;
    }

    private static Pair<BlobStoreLocation, BlobStore> createBlobStore(String driver,
                                                                      String region,
                                                                      String endpoint,
                                                                      Supplier<Credentials> credentials,
                                                                      int maxBlockSize) {
        Properties overrides = new Properties();
        // This property controls the number of parts being uploaded in parallel.
        overrides.setProperty("jclouds.mpu.parallel.degree", "1");
        overrides.setProperty("jclouds.mpu.parts.size", Integer.toString(maxBlockSize));
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));

        ApiRegistry.registerApi(new S3ApiMetadata());
        ProviderRegistry.registerProvider(new AWSS3ProviderMetadata());
        ProviderRegistry.registerProvider(new GoogleCloudStorageProviderMetadata());

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(driver);
        contextBuilder.credentialsSupplier(credentials);

        if (isS3Driver(driver) && !Strings.isNullOrEmpty(endpoint)) {
            contextBuilder.endpoint(endpoint);
            overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        }
        contextBuilder.overrides(overrides);
        BlobStoreContext context = contextBuilder.buildView(BlobStoreContext.class);
        BlobStore blobStore = context.getBlobStore();

        log.info("Connect to blobstore : driver: {}, region: {}, endpoint: {}",
            driver, region, endpoint);
        return Pair.of(
            BlobStoreLocation.of(region, endpoint),
            blobStore);
    }

    private final VersionCheck VERSION_CHECK = (key, blob) -> {
        // NOTE all metadata in jclouds comes out as lowercase, in an effort to normalize the providers
        String version = blob.getMetadata().getUserMetadata().get(METADATA_FORMAT_VERSION_KEY.toLowerCase());
        if (version == null || !version.equals(CURRENT_VERSION)) {
            throw new IOException(String.format("Invalid object version %s for %s, expect %s",
                version, key, CURRENT_VERSION));
        }
    };

    private final OrderedScheduler scheduler;

    // container in jclouds to write offloaded ledgers
    private final String writeBucket;
    // the region to write offloaded ledgers
    private final String writeRegion;
    // the endpoint
    private final String writeEndpoint;
    // credentials
    private final Supplier<Credentials> credentials;

    // max block size for each data block.
    private int maxBlockSize;
    private final int readBufferSize;

    private final BlobStore writeBlobStore;
    private final Location writeLocation;

    private final ConcurrentMap<BlobStoreLocation, BlobStore> readBlobStores = new ConcurrentHashMap<>();

    // metadata to be stored as part of the offloaded ledger metadata
    private final Map<String, String> userMetadata;
    // offload driver metadata to be stored as part of the original ledger metadata
    private final String offloadDriverName;

    private static OffloadPolicies offloadPolicies;

    @VisibleForTesting
    static BlobStoreManagedLedgerOffloader create(OffloadPolicies conf,
                                                  OrderedScheduler scheduler) throws IOException {
        return create(conf, Maps.newHashMap(), scheduler);
    }

    public static BlobStoreManagedLedgerOffloader create(OffloadPolicies conf,
                                                         Map<String, String> userMetadata,
                                                         OrderedScheduler scheduler)
            throws IOException {
        offloadPolicies = conf;
        String driver = conf.getManagedLedgerOffloadDriver();
        if (!driverSupported(driver)) {
            throw new IOException(
                "Not support this kind of driver as offload backend: " + driver);
        }

        String endpoint = conf.getS3ManagedLedgerOffloadServiceEndpoint();
        String region = isS3Driver(driver) ?
            conf.getS3ManagedLedgerOffloadRegion() :
            conf.getGcsManagedLedgerOffloadRegion();
        String bucket = isS3Driver(driver) ?
            conf.getS3ManagedLedgerOffloadBucket() :
            conf.getGcsManagedLedgerOffloadBucket();
        int maxBlockSize = isS3Driver(driver) ?
            conf.getS3ManagedLedgerOffloadMaxBlockSizeInBytes() :
            conf.getGcsManagedLedgerOffloadMaxBlockSizeInBytes();
        int readBufferSize = isS3Driver(driver) ?
            conf.getS3ManagedLedgerOffloadReadBufferSizeInBytes() :
            conf.getGcsManagedLedgerOffloadReadBufferSizeInBytes();

        if (isS3Driver(driver) && Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
            throw new IOException(
                    "Either s3ManagedLedgerOffloadRegion or s3ManagedLedgerOffloadServiceEndpoint must be set"
                    + " if s3 offload enabled");
        }

        if (Strings.isNullOrEmpty(bucket)) {
            throw new IOException(
                "ManagedLedgerOffloadBucket cannot be empty for s3 and gcs offload");
        }
        if (maxBlockSize < 5*1024*1024) {
            throw new IOException(
                "ManagedLedgerOffloadMaxBlockSizeInBytes cannot be less than 5MB for s3 and gcs offload");
        }

        Supplier<Credentials> credentials = getCredentials(driver, conf);

        return new BlobStoreManagedLedgerOffloader(driver, bucket, scheduler,
            maxBlockSize, readBufferSize, endpoint, region, credentials, userMetadata);
    }

    public static Supplier<Credentials> getCredentials(String driver,
               OffloadPolicies conf) throws IOException {
        // credentials:
        //   for s3, get by DefaultAWSCredentialsProviderChain.
        //   for gcs, use downloaded file 'google_creds.json', which contains service account key by
        //     following instructions in page https://support.google.com/googleapi/answer/6158849

        if (isGcsDriver(driver)) {
            String gcsKeyPath = conf.getGcsManagedLedgerOffloadServiceAccountKeyFile();
            if (Strings.isNullOrEmpty(gcsKeyPath)) {
                throw new IOException(
                    "The service account key path is empty for GCS driver");
            }
            try {
                String gcsKeyContent = Files.toString(new File(gcsKeyPath), Charset.defaultCharset());
                return () -> new GoogleCredentialsFromJson(gcsKeyContent).get();
            } catch (IOException ioe) {
                log.error("Cannot read GCS service account credentials file: {}", gcsKeyPath);
                throw new IOException(ioe);
            }
        } else if (isS3Driver(driver)) {
            AWSCredentialsProvider credsChain = CredentialsUtil.getAWSCredentialProvider(conf);
            // try and get creds before starting... if we can't fetch
            // creds on boot, we want to fail
            try {
                credsChain.getCredentials();
            } catch (Exception e) {
                // allowed, some mock s3 service not need credential
                log.error("unable to fetch S3 credentials for offloading, failing", e);
                throw e;
            }

            return () -> {
                AWSCredentials creds = credsChain.getCredentials();
                if (creds == null) {
                    // we don't expect this to happen, as we
                    // successfully fetched creds on boot
                    throw new RuntimeException("Unable to fetch S3 credentials after start, unexpected!");
                }
                // if we have session credentials, we need to send the session token
                // this allows us to support EC2 metadata credentials
                if (creds instanceof AWSSessionCredentials) {
                    return SessionCredentials.builder()
                            .accessKeyId(creds.getAWSAccessKeyId())
                            .secretAccessKey(creds.getAWSSecretKey())
                            .sessionToken(((AWSSessionCredentials) creds).getSessionToken())
                            .build();
                } else {
                    return new Credentials(creds.getAWSAccessKeyId(), creds.getAWSSecretKey());
                }
            };
        } else {
            throw new IOException(
                "Not support this kind of driver: " + driver);
        }
    }


    // build context for jclouds BlobStoreContext
    BlobStoreManagedLedgerOffloader(String driver, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize, String endpoint, String region, Supplier<Credentials> credentials) {
        this(driver, container, scheduler, maxBlockSize, readBufferSize, endpoint, region, credentials, Maps.newHashMap());
    }

    BlobStoreManagedLedgerOffloader(String driver, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize,
                                    String endpoint, String region, Supplier<Credentials> credentials,
                                    Map<String, String> userMetadata) {
        this.offloadDriverName = driver;
        this.scheduler = scheduler;
        this.readBufferSize = readBufferSize;
        this.writeBucket = container;
        this.writeRegion = region;
        this.writeEndpoint = endpoint;
        this.maxBlockSize = maxBlockSize;
        this.userMetadata = userMetadata;
        this.credentials = credentials;

        if (!Strings.isNullOrEmpty(region)) {
            this.writeLocation = new LocationBuilder()
                .scope(LocationScope.REGION)
                .id(region)
                .description(region)
                .build();
        } else {
            this.writeLocation = null;
        }

        log.info("Constructor offload driver: {}, host: {}, container: {}, region: {} ",
            driver, endpoint, container, region);

        Pair<BlobStoreLocation, BlobStore> blobStore = createBlobStore(
            driver, region, endpoint, credentials, maxBlockSize
        );
        this.writeBlobStore = blobStore.getRight();
        this.readBlobStores.put(blobStore.getLeft(), blobStore.getRight());
    }

    // build context for jclouds BlobStoreContext, mostly used in test
    @VisibleForTesting
    BlobStoreManagedLedgerOffloader(BlobStore blobStore, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize) {
        this(blobStore, container, scheduler, maxBlockSize, readBufferSize, Maps.newHashMap());
    }

    BlobStoreManagedLedgerOffloader(BlobStore blobStore, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize,
                                    Map<String, String> userMetadata) {
        this.offloadDriverName = "aws-s3";
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
            BlobStoreLocation.of(writeRegion, writeEndpoint),
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
        return offloadDriverName;
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        return ImmutableMap.of(
            METADATA_FIELD_BUCKET, writeBucket,
            METADATA_FIELD_REGION, Strings.nullToEmpty(writeRegion),
            METADATA_FIELD_ENDPOINT, Strings.nullToEmpty(writeEndpoint)
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
                        partPayload.getContentMetadata().setContentLength((long)blockSize);
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
                indexPayload.getContentMetadata().setContentLength(indexStream.getStreamSize());
                indexPayload.getContentMetadata().setContentType("application/octet-stream");

                Blob blob = blobBuilder
                    .payload(indexPayload)
                    .contentLength(indexStream.getStreamSize())
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
            getReadRegion(offloadDriverMetadata),
            getReadEndpoint(offloadDriverMetadata)
        );
        BlobStore blobStore = readBlobStores.get(location);
        if (null == blobStore) {
            blobStore = createBlobStore(
                offloadDriverName,
                location.getRegion(),
                location.getEndpoint(),
                credentials,
                maxBlockSize
            ).getRight();
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

    public interface VersionCheck {
        void check(String key, Blob blob) throws IOException;
    }

    @Override
    public OffloadPolicies getOffloadPolicies() {
        return offloadPolicies;
    }

    @Override
    public void close() {
        if (writeBlobStore != null) {
            writeBlobStore.getContext().close();
        }
        for (BlobStore readBlobStore : readBlobStores.values()) {
            if (readBlobStore != null) {
                readBlobStore.getContext().close();
            }
        }
    }
}


