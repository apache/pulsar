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
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.base.Strings;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.s3offload.impl.BlockAwareSegmentInputStreamImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ManagedLedgerOffloader implements LedgerOffloader {
    private static final Logger log = LoggerFactory.getLogger(S3ManagedLedgerOffloader.class);

    public static final String DRIVER_NAME = "S3";
    private final ScheduledExecutorService scheduler;
    private final AmazonS3 s3client;
    private final String bucket;
    private int maxBlockSize;

    public static S3ManagedLedgerOffloader create(ServiceConfiguration conf,
                                                  ScheduledExecutorService scheduler)
            throws PulsarServerException {
        String region = conf.getS3ManagedLedgerOffloadRegion();
        String bucket = conf.getS3ManagedLedgerOffloadBucket();
        String endpoint = conf.getS3ManagedLedgerOffloadServiceEndpoint();
        if (Strings.isNullOrEmpty(region)) {
            throw new PulsarServerException("s3ManagedLedgerOffloadRegion cannot be empty is s3 offload enabled");
        }
        if (Strings.isNullOrEmpty(bucket)) {
            throw new PulsarServerException("s3ManagedLedgerOffloadBucket cannot be empty is s3 offload enabled");
        }

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        if (!Strings.isNullOrEmpty(endpoint)) {
            builder.setEndpointConfiguration(new EndpointConfiguration(endpoint, region));
            builder.setPathStyleAccessEnabled(true);
        } else {
            builder.setRegion(region);
        }
        S3ManagedLedgerOffloader offloader = new S3ManagedLedgerOffloader(builder.build(), bucket, scheduler);
        offloader.maxBlockSize = conf.getS3ManagedLedgerOffloadMaxBlockSizeInBytes();
        return offloader;
    }

    S3ManagedLedgerOffloader(AmazonS3 s3client, String bucket, ScheduledExecutorService scheduler) {
        this.s3client = s3client;
        this.bucket = bucket;
        this.scheduler = scheduler;
    }

    static String offloadKey(ReadHandle readHandle, UUID uuid) {
        return String.format("ledger-%d-%s", readHandle.getId(), uuid.toString());
    }

    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle,
                                           UUID uuid,
                                           Map<String, String> extraMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.submit(() -> {
            try {
                String key = offloadKey(readHandle, uuid);
                InitiateMultipartUploadResult initRes = s3client.initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(this.bucket, key));
                OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create()
                    .withMetadata(readHandle.getLedgerMetadata());

                long startEntry = 0;
                int partId = 1;
                long bytesUploaded = 0;
                List<PartETag> etags = new LinkedList<>();
                while (startEntry <= readHandle.getLastAddConfirmed()) {
                    int blockSize = BlockAwareSegmentInputStreamImpl
                        .getBlockSize(readHandle, maxBlockSize, startEntry, bytesUploaded);

                    BlockAwareSegmentInputStreamImpl blockStream = new BlockAwareSegmentInputStreamImpl(
                        readHandle, startEntry, blockSize);

                    UploadPartResult uploadRes = s3client.uploadPart(
                        new UploadPartRequest()
                            .withBucketName(bucket)
                            .withKey(key)
                            .withUploadId(initRes.getUploadId())
                            .withInputStream(blockStream)
                            .withPartSize(blockSize)
                            .withPartNumber(partId));
                    etags.add(uploadRes.getPartETag());

                    indexBuilder.addBlock(startEntry, partId, blockSize);

                    bytesUploaded += blockStream.getBlockEntryBytesCount();
                    startEntry = blockStream.getEndEntryId() + 1;
                    partId++;
                    blockStream.close();
                }

                try (OffloadIndexBlock index = indexBuilder.build()) {
                    InputStream indexStream = index.toStream();
                    // write the index
                    UploadPartResult uploadRes = s3client.uploadPart(
                        new UploadPartRequest()
                            .withBucketName(bucket)
                            .withKey(key)
                            .withUploadId(initRes.getUploadId())
                            .withInputStream(indexStream)
                            .withPartSize(indexStream.available())
                            .withPartNumber(1));
                    etags.add(uploadRes.getPartETag());
                    indexStream.close();
                }

                s3client.completeMultipartUpload(new CompleteMultipartUploadRequest()
                    .withBucketName(bucket).withKey(key)
                    .withUploadId(initRes.getUploadId())
                    .withPartETags(etags));

                //s3client.putObject(bucket, uid.toString(), uid.toString());
                promise.complete(null);
            } catch (Throwable t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid) {
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }
}


