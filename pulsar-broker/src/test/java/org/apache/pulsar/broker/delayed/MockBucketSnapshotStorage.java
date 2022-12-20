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
package org.apache.pulsar.broker.delayed;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.delayed.bucket.BucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;

@Slf4j
public class MockBucketSnapshotStorage implements BucketSnapshotStorage {

    private final AtomicLong maxBucketId;

    private final Map<Long, List<ByteBuf>> bucketSnapshots;

    private final ExecutorService executorService =
            new ThreadPoolExecutor(10, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                    new DefaultThreadFactory("bucket-snapshot-storage-io"));

    public MockBucketSnapshotStorage() {
        this.bucketSnapshots = new ConcurrentHashMap<>();
        this.maxBucketId = new AtomicLong();
    }

    @Override
    public CompletableFuture<Long> createBucketSnapshot(
            SnapshotMetadata snapshotMetadata, List<SnapshotSegment> bucketSnapshotSegments, String bucketKey) {
        return CompletableFuture.supplyAsync(() -> {
            long bucketId = maxBucketId.getAndIncrement();
            List<ByteBuf> entries = new ArrayList<>();
            byte[] bytes = snapshotMetadata.toByteArray();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(bytes.length);
            byteBuf.writeBytes(bytes);
            entries.add(byteBuf);
            this.bucketSnapshots.put(bucketId, entries);
            return bucketId;
        }, executorService).thenApply(bucketId -> {
            List<ByteBuf> bufList = new ArrayList<>();
            for (SnapshotSegment snapshotSegment : bucketSnapshotSegments) {
                byte[] bytes = snapshotSegment.toByteArray();
                ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(bytes.length);
                byteBuf.writeBytes(bytes);
                bufList.add(byteBuf);
            }
            bucketSnapshots.get(bucketId).addAll(bufList);

            return bucketId;
        });
    }

    @Override
    public CompletableFuture<SnapshotMetadata> getBucketSnapshotMetadata(long bucketId) {
        return CompletableFuture.supplyAsync(() -> {
            ByteBuf byteBuf = this.bucketSnapshots.get(bucketId).get(0);
            SnapshotMetadata snapshotMetadata;
            try {
                snapshotMetadata = SnapshotMetadata.parseFrom(byteBuf.nioBuffer());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            return snapshotMetadata;
        }, executorService);
    }

    @Override
    public CompletableFuture<List<SnapshotSegment>> getBucketSnapshotSegment(long bucketId, long firstSegmentEntryId,
                                                                             long lastSegmentEntryId) {
        return CompletableFuture.supplyAsync(() -> {
            List<SnapshotSegment> snapshotSegments = new ArrayList<>();
            long lastEntryId = Math.min(lastSegmentEntryId, this.bucketSnapshots.get(bucketId).size());
            for (int i = (int) firstSegmentEntryId; i <= lastEntryId ; i++) {
                ByteBuf byteBuf = this.bucketSnapshots.get(bucketId).get(i);
                SnapshotSegment snapshotSegment;
                try {
                    snapshotSegment = SnapshotSegment.parseFrom(byteBuf.nioBuffer());
                    snapshotSegments.add(snapshotSegment);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            return snapshotSegments;
        }, executorService);
    }

    @Override
    public CompletableFuture<Void> deleteBucketSnapshot(long bucketId) {
        return CompletableFuture.supplyAsync(() -> {
            List<ByteBuf> remove = this.bucketSnapshots.remove(bucketId);
            if (remove != null) {
                for (ByteBuf byteBuf : remove) {
                    byteBuf.release();
                }
            }
            return null;
        }, executorService);
    }

    @Override
    public CompletableFuture<Long> getBucketSnapshotLength(long bucketId) {
        return CompletableFuture.supplyAsync(() -> {
            long length = 0;
            List<ByteBuf> bufList = this.bucketSnapshots.get(bucketId);
            for (ByteBuf byteBuf : bufList) {
                length += byteBuf.array().length;
            }
            return length;
        }, executorService);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {
        clean();
    }

    public void clean() {
        for (List<ByteBuf> value : bucketSnapshots.values()) {
            for (ByteBuf byteBuf : value) {
                byteBuf.release();
            }
        }
        bucketSnapshots.clear();
        executorService.shutdownNow();
    }
}
