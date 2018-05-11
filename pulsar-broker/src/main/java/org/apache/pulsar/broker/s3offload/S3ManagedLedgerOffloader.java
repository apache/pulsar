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

import com.google.common.base.Strings;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;

public class S3ManagedLedgerOffloader implements LedgerOffloader {
    public static final String DRIVER_NAME = "S3";
    private final ScheduledExecutorService scheduler;
    private final AmazonS3 s3client;
    private final String bucket;

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
        return new S3ManagedLedgerOffloader(builder.build(), bucket, scheduler);
    }

    S3ManagedLedgerOffloader(AmazonS3 s3client, String bucket, ScheduledExecutorService scheduler) {
        this.s3client = s3client;
        this.bucket = bucket;
        this.scheduler = scheduler;
    }

    @Override
    public CompletableFuture<Void> offload(ReadHandle ledger,
                                           UUID uid,
                                           Map<String, String> extraMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.submit(() -> {
                try {
                    s3client.putObject(bucket, uid.toString(), uid.toString());
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


