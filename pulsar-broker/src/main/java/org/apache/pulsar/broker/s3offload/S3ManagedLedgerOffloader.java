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
package org.apache.pulsar.broker.s3offload;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.base.Strings;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.s3offload.impl.BlockAwareSegmentInputStreamImpl;
import org.apache.pulsar.broker.s3offload.impl.S3BackedReadHandleImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ManagedLedgerOffloader implements LedgerOffloader {
    private static final Logger log = LoggerFactory.getLogger(S3ManagedLedgerOffloader.class);

    public static final String DRIVER_NAME = "S3";
    private final OrderedScheduler scheduler;
    private final AmazonS3 s3client;
    private final String bucket;
    // max block size for each data block.
    private int maxBlockSize;
    private final int readBufferSize;

    public static S3ManagedLedgerOffloader create(ServiceConfiguration conf,
                                                  OrderedScheduler scheduler)
            throws PulsarServerException {
        String region = conf.getS3ManagedLedgerOffloadRegion();
        String bucket = conf.getS3ManagedLedgerOffloadBucket();
        String endpoint = conf.getS3ManagedLedgerOffloadServiceEndpoint();
        int maxBlockSize = conf.getS3ManagedLedgerOffloadMaxBlockSizeInBytes();
        int readBufferSize = conf.getS3ManagedLedgerOffloadReadBufferSizeInBytes();

        if (Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
            throw new PulsarServerException(
                    "Either s3ManagedLedgerOffloadRegion or s3ManagedLedgerOffloadServiceEndpoint must be set"
                    + " if s3 offload enabled");
        }
        if (Strings.isNullOrEmpty(bucket)) {
            throw new PulsarServerException("s3ManagedLedgerOffloadBucket cannot be empty if s3 offload enabled");
        }
        if (maxBlockSize < 5*1024*1024) {
            throw new PulsarServerException("s3ManagedLedgerOffloadMaxBlockSizeInBytes cannot be less than 5MB");
        }

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        if (!Strings.isNullOrEmpty(endpoint)) {
            builder.setEndpointConfiguration(new EndpointConfiguration(endpoint, region));
            builder.setPathStyleAccessEnabled(true);
        } else {
            builder.setRegion(region);
        }
        return new S3ManagedLedgerOffloader(builder.build(), bucket, scheduler, maxBlockSize, readBufferSize);
    }

    S3ManagedLedgerOffloader(AmazonS3 s3client, String bucket, OrderedScheduler scheduler,
                             int maxBlockSize, int readBufferSize) {
        this.s3client = s3client;
        this.bucket = bucket;
        this.scheduler = scheduler;
        this.maxBlockSize = maxBlockSize;
        this.readBufferSize = readBufferSize;
    }

    static String dataBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("ledger-%d-%s", ledgerId, uuid.toString());
    }

    static String indexBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("ledger-%d-%s-index", ledgerId, uuid.toString());
    }

    // upload DataBlock to s3 using MultiPartUpload, and indexBlock in a new Block,
    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle,
                                           UUID uuid,
                                           Map<String, String> extraMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).submit(() -> {
            OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create()
                .withLedgerMetadata(readHandle.getLedgerMetadata());
            String dataBlockKey = dataBlockOffloadKey(readHandle.getId(), uuid);
            String indexBlockKey = indexBlockOffloadKey(readHandle.getId(), uuid);
            InitiateMultipartUploadRequest dataBlockReq = new InitiateMultipartUploadRequest(bucket, dataBlockKey, new ObjectMetadata());
            InitiateMultipartUploadResult dataBlockRes = null;

            // init multi part upload for data block.
            try {
                dataBlockRes = s3client.initiateMultipartUpload(dataBlockReq);
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
                List<PartETag> etags = new LinkedList<>();
                while (startEntry <= readHandle.getLastAddConfirmed()) {
                    int blockSize = BlockAwareSegmentInputStreamImpl
                        .calculateBlockSize(maxBlockSize, readHandle, startEntry, entryBytesWritten);

                    try (BlockAwareSegmentInputStream blockStream = new BlockAwareSegmentInputStreamImpl(
                        readHandle, startEntry, blockSize)) {

                        UploadPartResult uploadRes = s3client.uploadPart(
                            new UploadPartRequest()
                                .withBucketName(bucket)
                                .withKey(dataBlockKey)
                                .withUploadId(dataBlockRes.getUploadId())
                                .withInputStream(blockStream)
                                .withPartSize(blockSize)
                                .withPartNumber(partId));
                        etags.add(uploadRes.getPartETag());
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

                s3client.completeMultipartUpload(new CompleteMultipartUploadRequest()
                    .withBucketName(bucket).withKey(dataBlockKey)
                    .withUploadId(dataBlockRes.getUploadId())
                    .withPartETags(etags));
            } catch (Throwable t) {
                try {
                    s3client.abortMultipartUpload(
                        new AbortMultipartUploadRequest(bucket, dataBlockKey, dataBlockRes.getUploadId()));
                } catch (Throwable throwable) {
                    log.error("Failed abortMultipartUpload ", throwable);
                }
                promise.completeExceptionally(t);
                return;
            }

            // upload index block
            try (OffloadIndexBlock index = indexBuilder.withDataObjectLength(dataObjectLength).build();
                 OffloadIndexBlock.IndexInputStream indexStream = index.toStream()) {
                // write the index block
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(indexStream.getStreamSize());
                s3client.putObject(new PutObjectRequest(
                    bucket,
                    indexBlockKey,
                    indexStream,
                    metadata));
                promise.complete(null);
            } catch (Throwable t) {
                try {
                    s3client.deleteObject(bucket, dataBlockKey);
                } catch (Throwable throwable) {
                    log.error("Failed deleteObject ", throwable);
                }
                promise.completeExceptionally(t);
                return;
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid) {
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String key = dataBlockOffloadKey(ledgerId, uid);
        String indexKey = indexBlockOffloadKey(ledgerId, uid);
        scheduler.chooseThread(ledgerId).submit(() -> {
                try {
                    promise.complete(S3BackedReadHandleImpl.open(scheduler.chooseThread(ledgerId),
                                                                 s3client,
                                                                 bucket, key, indexKey,
                                                                 ledgerId, readBufferSize));
                } catch (Throwable t) {
                    promise.completeExceptionally(t);
                }
            });
        return promise;
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                s3client.deleteObjects(new DeleteObjectsRequest(bucket)
                    .withKeys(dataBlockOffloadKey(ledgerId, uid), indexBlockOffloadKey(ledgerId, uid)));
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete s3 Object ", t);
                promise.completeExceptionally(t);
            }
        });

        return promise;
    }
}


