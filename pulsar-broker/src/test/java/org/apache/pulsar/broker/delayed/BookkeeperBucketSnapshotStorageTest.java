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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.delayed.bucket.BookkeeperBucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegmentMetadata;
import org.apache.pulsar.common.util.FutureUtil;
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

    private static final String TOPIC_NAME = "topicName";
    private static final String CURSOR_NAME = "sub";

    @Test
    public void testCreateSnapshot() throws ExecutionException, InterruptedException {
        SnapshotMetadata snapshotMetadata = new SnapshotMetadata();
        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString(), TOPIC_NAME, CURSOR_NAME);
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);
    }

    @Test
    public void testGetSnapshot() throws ExecutionException, InterruptedException {
        SnapshotSegmentMetadata segmentMetadata = new SnapshotSegmentMetadata();
        segmentMetadata.setMinScheduleTimestamp(System.currentTimeMillis());
        segmentMetadata.setMaxScheduleTimestamp(System.currentTimeMillis());
        segmentMetadata.putDelayedIndexBitMap(100L, new byte[1]);

        SnapshotMetadata snapshotMetadata = new SnapshotMetadata();
        snapshotMetadata.addMetadata().copyFrom(segmentMetadata);

        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();

        long timeMillis = System.currentTimeMillis();
        DelayedIndex delayedIndex = new DelayedIndex().setLedgerId(100L).setEntryId(10L)
                        .setTimestamp(timeMillis);
        SnapshotSegment snapshotSegment = new SnapshotSegment();
        snapshotSegment.addIndexe().copyFrom(delayedIndex);
        bucketSnapshotSegments.add(snapshotSegment);
        bucketSnapshotSegments.add(snapshotSegment);

        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString(), TOPIC_NAME, CURSOR_NAME);
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);

        CompletableFuture<List<SnapshotSegment>> bucketSnapshotSegment =
                bucketSnapshotStorage.getBucketSnapshotSegment(bucketId, 1, 3);

        List<SnapshotSegment> snapshotSegments = bucketSnapshotSegment.get();
        Assert.assertEquals(2, snapshotSegments.size());
        for (SnapshotSegment segment : snapshotSegments) {
            for (int i = 0; i < segment.getIndexesCount(); i++) {
                DelayedIndex index = segment.getIndexeAt(i);
                Assert.assertEquals(100L, index.getLedgerId());
                Assert.assertEquals(10L, index.getEntryId());
                Assert.assertEquals(timeMillis, index.getTimestamp());
            }
        }
    }

    @Test
    public void testGetSnapshotMetadata() throws ExecutionException, InterruptedException {
        long timeMillis = System.currentTimeMillis();

        SnapshotSegmentMetadata segmentMetadata = new SnapshotSegmentMetadata();
        segmentMetadata.setMaxScheduleTimestamp(timeMillis);
        segmentMetadata.setMinScheduleTimestamp(timeMillis);
        segmentMetadata.putDelayedIndexBitMap(100L, "test1".getBytes(StandardCharsets.UTF_8));
        segmentMetadata.putDelayedIndexBitMap(200L, "test2".getBytes(StandardCharsets.UTF_8));

        SnapshotMetadata snapshotMetadata = new SnapshotMetadata();
        snapshotMetadata.addMetadata().copyFrom(segmentMetadata);

        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();

        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString(), TOPIC_NAME, CURSOR_NAME);
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);

        SnapshotMetadata bucketSnapshotMetadata =
                bucketSnapshotStorage.getBucketSnapshotMetadata(bucketId).get();

        SnapshotSegmentMetadata metadata =
                bucketSnapshotMetadata.getMetadataAt(0);

        Assert.assertEquals(timeMillis, metadata.getMaxScheduleTimestamp());
        Assert.assertEquals("test1", new String(metadata.getDelayedIndexBitMap(100L), StandardCharsets.UTF_8));
        Assert.assertEquals("test2", new String(metadata.getDelayedIndexBitMap(200L), StandardCharsets.UTF_8));
    }

    @Test
    public void testDeleteSnapshot() throws ExecutionException, InterruptedException {
        SnapshotMetadata snapshotMetadata = new SnapshotMetadata();
        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString(), TOPIC_NAME, CURSOR_NAME);
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
        SnapshotSegmentMetadata segmentMetadata = new SnapshotSegmentMetadata();
        segmentMetadata.setMinScheduleTimestamp(System.currentTimeMillis());
        segmentMetadata.setMaxScheduleTimestamp(System.currentTimeMillis());
        segmentMetadata.putDelayedIndexBitMap(100L, new byte[1]);

        SnapshotMetadata snapshotMetadata = new SnapshotMetadata();
        snapshotMetadata.addMetadata().copyFrom(segmentMetadata);

        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();

        long timeMillis = System.currentTimeMillis();
        DelayedIndex delayedIndex = new DelayedIndex().setLedgerId(100L).setEntryId(10L).setTimestamp(timeMillis);
        SnapshotSegment snapshotSegment = new SnapshotSegment();
        snapshotSegment.addIndexe().copyFrom(delayedIndex);
        bucketSnapshotSegments.add(snapshotSegment);
        bucketSnapshotSegments.add(snapshotSegment);

        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString(), TOPIC_NAME, CURSOR_NAME);
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);

        Long bucketSnapshotLength = bucketSnapshotStorage.getBucketSnapshotLength(bucketId).get();
        System.out.println(bucketSnapshotLength);
        Assert.assertTrue(bucketSnapshotLength > 0L);
    }

    @Test
    public void testConcurrencyGet() throws ExecutionException, InterruptedException {
        SnapshotSegmentMetadata segmentMetadata = new SnapshotSegmentMetadata();
        segmentMetadata.setMinScheduleTimestamp(System.currentTimeMillis());
        segmentMetadata.setMaxScheduleTimestamp(System.currentTimeMillis());
        segmentMetadata.putDelayedIndexBitMap(100L, new byte[1]);

        SnapshotMetadata snapshotMetadata = new SnapshotMetadata();
        snapshotMetadata.addMetadata().copyFrom(segmentMetadata);

        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();

        long timeMillis = System.currentTimeMillis();
        DelayedIndex delayedIndex = new DelayedIndex().setLedgerId(100L).setEntryId(10L).setTimestamp(timeMillis);
        SnapshotSegment snapshotSegment = new SnapshotSegment();
        snapshotSegment.addIndexe().copyFrom(delayedIndex);
        bucketSnapshotSegments.add(snapshotSegment);
        bucketSnapshotSegments.add(snapshotSegment);

        CompletableFuture<Long> future =
                bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata,
                        bucketSnapshotSegments, UUID.randomUUID().toString(), TOPIC_NAME, CURSOR_NAME);
        Long bucketId = future.get();
        Assert.assertNotNull(bucketId);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            CompletableFuture<Void> future0 = CompletableFuture.runAsync(() -> {
                List<SnapshotSegment> list =
                        bucketSnapshotStorage.getBucketSnapshotSegment(bucketId, 1, 3).join();
                Assert.assertTrue(list.size() > 0);
            });
            futures.add(future0);
        }

        FutureUtil.waitForAll(futures).join();
    }

}
