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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;

public class MockBucketSnapshotStorage implements BucketSnapshotStorage {

    private final AtomicLong idGenerator = new AtomicLong(1);
    private final Map<Long, SnapshotMetadata> snapshots = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Long> createBucketSnapshot(SnapshotMetadata snapshotMetadata,
                                                        List<SnapshotSegment> bucketSnapshotSegments,
                                                        String bucketKey, String topicName, String cursorName) {
        long id = idGenerator.getAndIncrement();
        snapshots.put(id, snapshotMetadata);
        return CompletableFuture.completedFuture(id);
    }

    @Override
    public CompletableFuture<SnapshotMetadata> getBucketSnapshotMetadata(long bucketId) {
        SnapshotMetadata metadata = snapshots.get(bucketId);
        return CompletableFuture.completedFuture(metadata);
    }

    @Override
    public CompletableFuture<List<SnapshotSegment>> getBucketSnapshotSegment(long bucketId,
                                                                             long firstSegmentEntryId,
                                                                             long lastSegmentEntryId) {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<Long> getBucketSnapshotLength(long bucketId) {
        return CompletableFuture.completedFuture(0L);
    }

    @Override
    public CompletableFuture<Void> deleteBucketSnapshot(long bucketId) {
        snapshots.remove(bucketId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void start() throws Exception {
        // No-op
    }

    @Override
    public void close() throws Exception {
        snapshots.clear();
    }
}
