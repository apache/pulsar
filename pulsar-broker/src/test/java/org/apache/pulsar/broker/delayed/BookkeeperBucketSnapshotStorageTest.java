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

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.delayed.bucket.BookkeeperBucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BookkeeperBucketSnapshotStorageTest extends MockedPulsarServiceBaseTest {

    private BookkeeperBucketSnapshotStorage bucketSnapshotStorage;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        bucketSnapshotStorage = new BookkeeperBucketSnapshotStorage(pulsar);
        bucketSnapshotStorage.start();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        bucketSnapshotStorage.close();
    }

    @Test
    public void testCreateSnapshot() throws ExecutionException, InterruptedException {
        DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata snapshotMetadata =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata.newBuilder().build();
        List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString());
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);
    }

    @Test
    public void testGetSnapshot() throws ExecutionException, InterruptedException {
        DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata segmentMetadata =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata.newBuilder()
                        .setMaxScheduleTimestamp(System.currentTimeMillis())
                        .putDelayedIndexBitMap(100L, ByteString.copyFrom(new byte[1])).build();

        DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata snapshotMetadata =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata.newBuilder()
                        .addMetadataList(segmentMetadata)
                        .build();
        List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();

        long timeMillis = System.currentTimeMillis();
        DelayedMessageIndexBucketSnapshotFormat.DelayedIndex delayedIndex =
                DelayedMessageIndexBucketSnapshotFormat.DelayedIndex.newBuilder().setLedgerId(100L).setEntryId(10L)
                        .setTimestamp(timeMillis).build();
        DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment snapshotSegment =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment.newBuilder().addIndexes(delayedIndex).build();
        bucketSnapshotSegments.add(snapshotSegment);
        bucketSnapshotSegments.add(snapshotSegment);

        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString());
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);

        CompletableFuture<List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment>> bucketSnapshotSegment =
                bucketSnapshotStorage.getBucketSnapshotSegment(bucketId, 1, 3);

        List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment> snapshotSegments = bucketSnapshotSegment.get();
        Assert.assertEquals(2, snapshotSegments.size());
        for (DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment segment : snapshotSegments) {
            for (DelayedMessageIndexBucketSnapshotFormat.DelayedIndex index : segment.getIndexesList()) {
                Assert.assertEquals(100L, index.getLedgerId());
                Assert.assertEquals(10L, index.getEntryId());
                Assert.assertEquals(timeMillis, index.getTimestamp());
            }
        }
    }

    @Test
    public void testGetSnapshotMetadata() throws ExecutionException, InterruptedException {
        long timeMillis = System.currentTimeMillis();

        Map<Long, ByteString> map = new HashMap<>();
        map.put(100L, ByteString.copyFrom("test1", StandardCharsets.UTF_8));
        map.put(200L, ByteString.copyFrom("test2", StandardCharsets.UTF_8));

        DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata segmentMetadata =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata.newBuilder()
                        .setMaxScheduleTimestamp(timeMillis)
                        .putAllDelayedIndexBitMap(map).build();

        DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata snapshotMetadata =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata.newBuilder()
                        .addMetadataList(segmentMetadata)
                        .build();
        List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();

        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString());
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);

        DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata bucketSnapshotMetadata =
                bucketSnapshotStorage.getBucketSnapshotMetadata(bucketId).get();

        DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata metadata =
                bucketSnapshotMetadata.getMetadataList(0);

        Assert.assertEquals(timeMillis, metadata.getMaxScheduleTimestamp());
        Assert.assertEquals("test1", metadata.getDelayedIndexBitMapMap().get(100L).toStringUtf8());
        Assert.assertEquals("test2", metadata.getDelayedIndexBitMapMap().get(200L).toStringUtf8());
    }

    @Test
    public void testDeleteSnapshot() throws ExecutionException, InterruptedException {
        DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata snapshotMetadata =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata.newBuilder().build();
        List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString());
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);

        bucketSnapshotStorage.deleteBucketSnapshot(bucketId).get();

        try {
            bucketSnapshotStorage.getBucketSnapshotMetadata(bucketId).get();
            Assert.fail("Should fail");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("No such ledger exists"));
        }
    }

    @Test
    public void testGetBucketSnapshotLength() throws ExecutionException, InterruptedException {
        DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata segmentMetadata =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata.newBuilder()
                        .setMaxScheduleTimestamp(System.currentTimeMillis())
                        .putDelayedIndexBitMap(100L, ByteString.copyFrom(new byte[1])).build();

        DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata snapshotMetadata =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata.newBuilder()
                        .addMetadataList(segmentMetadata)
                        .build();
        List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();

        long timeMillis = System.currentTimeMillis();
        DelayedMessageIndexBucketSnapshotFormat.DelayedIndex delayedIndex =
                DelayedMessageIndexBucketSnapshotFormat.DelayedIndex.newBuilder().setLedgerId(100L).setEntryId(10L)
                        .setTimestamp(timeMillis).build();
        DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment snapshotSegment =
                DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment.newBuilder().addIndexes(delayedIndex).build();
        bucketSnapshotSegments.add(snapshotSegment);
        bucketSnapshotSegments.add(snapshotSegment);

        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString());
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);

        Long bucketSnapshotLength = bucketSnapshotStorage.getBucketSnapshotLength(bucketId).get();
        System.out.println(bucketSnapshotLength);
        Assert.assertTrue(bucketSnapshotLength > 0L);
    }

}
