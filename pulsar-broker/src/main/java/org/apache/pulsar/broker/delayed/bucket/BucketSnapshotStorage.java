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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;

public interface BucketSnapshotStorage {

    /**
     * Create a delayed message index bucket snapshot with metadata and bucketSnapshotSegments.
     *
     * @param snapshotMetadata the metadata of snapshot
     * @param bucketSnapshotSegments the list of snapshot segments
     * @param bucketKey the key of bucket is used to generate custom storage metadata
     * @return the future with bucketId(ledgerId).
     */
    CompletableFuture<Long> createBucketSnapshot(SnapshotMetadata snapshotMetadata,
                                                 List<SnapshotSegment> bucketSnapshotSegments,
                                                 String bucketKey);

    /**
     * Get delayed message index bucket snapshot metadata.
     *
     * @param bucketId the bucketId of snapshot
     * @return the future with snapshot expanded metadata
     */
    CompletableFuture<SnapshotMetadata> getBucketSnapshotMetadata(long bucketId);

    /**
     * Get a sequence of delayed message index bucket snapshot segments.
     *
     * @param bucketId the bucketId of snapshot
     * @param firstSegmentEntryId entryId of first segment of sequence (include)
     * @param lastSegmentEntryId entryId of last segment of sequence (include)
     * @return the future with snapshot segment
     */
    CompletableFuture<List<SnapshotSegment>> getBucketSnapshotSegment(long bucketId, long firstSegmentEntryId,
                                                                      long lastSegmentEntryId);

    /**
     * Get total byte length of delayed message index bucket snapshot.
     *
     * @param bucketId the bucketId of snapshot
     * @return the future with byte length of snapshot
     */
    CompletableFuture<Long> getBucketSnapshotLength(long bucketId);

    /**
     * Delete delayed message index bucket snapshot by bucketId.
     *
     * @param bucketId the bucketId of snapshot
     */
    CompletableFuture<Void> deleteBucketSnapshot(long bucketId);

    /**
     * Start the bucket snapshot storage service.
     */
    void start() throws Exception;

    /**
     * Close the bucket snapshot storage service.
     */
    void close() throws Exception;
}
