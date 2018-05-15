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

import static org.apache.pulsar.broker.s3offload.S3ManagedLedgerOffloader.dataBlockOffloadKey;
import static org.apache.pulsar.broker.s3offload.S3ManagedLedgerOffloader.indexBlockOffloadKey;
import static org.mockito.Matchers.any;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.DataInputStream;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.MockBookKeeper;
import org.apache.bookkeeper.client.MockLedgerHandle;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.s3offload.impl.BlockAwareSegmentInputStreamImpl;
import org.apache.pulsar.broker.s3offload.impl.DataBlockHeaderImpl;
import org.apache.pulsar.broker.s3offload.impl.OffloadIndexBlockImpl;
import org.apache.pulsar.broker.s3offload.impl.OffloadIndexTest.LedgerMetadataMock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

class S3ManagedLedgerOffloaderTest extends S3TestBase {
    final ScheduledExecutorService scheduler;
    final MockBookKeeper bk;

    S3ManagedLedgerOffloaderTest() throws Exception {
        scheduler = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("offloader-"));
        bk = new MockBookKeeper(MockedPulsarServiceBaseTest.createMockZooKeeper());
    }

    private ReadHandle buildReadHandle(int entryCount) throws Exception {
        MockLedgerHandle lh = (MockLedgerHandle)bk.createLedger(1,1,1, BookKeeper.DigestType.CRC32, "foobar".getBytes());

        for (int index = 0; index < entryCount; index ++) {
            lh.addEntry(("foooobarrr").getBytes()); // add entry with 10 bytes data
        }

        lh.close();

        // mock ledgerMetadata, so the lac in metadata is not -1;
        MockLedgerHandle spy = Mockito.spy(lh);
        LedgerMetadataMock metadata = new LedgerMetadataMock(1, 1, 1,
            DigestType.CRC32C, "foobar".getBytes(), null, false);
        metadata.setLastEntryId(entryCount - 1);
        Mockito.when(spy.getLedgerMetadata()).thenReturn(metadata);

        return spy;
    }

    private void verifyS3ObjectRead(S3Object object, S3Object indexObject, ReadHandle readHandle, int indexEntryCount, int entryCount, int maxBlockSize) throws Exception {
        DataInputStream dis = new DataInputStream(object.getObjectContent());
        int isLength = dis.available();

        // read out index block
        DataInputStream indexBlockIs = new DataInputStream(indexObject.getObjectContent());
        OffloadIndexBlock indexBlock = OffloadIndexBlockImpl.get(indexBlockIs);

        // 1. verify index block with passed in index entry count
        Assert.assertEquals(indexBlock.getEntryCount(), indexEntryCount);

        // 2. verify index block read out each indexEntry.
        int entryIdTracker = 0;
        int startPartIdTracker = 1;
        int startOffsetTracker = 0;
        long entryBytesUploaded = 0;
        int entryLength = 10;
        for (int i = 0; i < indexEntryCount; i ++) {
            // 2.1 verify each indexEntry in header block
            OffloadIndexEntry indexEntry = indexBlock.getIndexEntryForEntry(entryIdTracker);

            Assert.assertEquals(indexEntry.getPartId(), startPartIdTracker);
            Assert.assertEquals(indexEntry.getEntryId(), entryIdTracker);
            Assert.assertEquals(indexEntry.getOffset(), startOffsetTracker);

            // read out and verify each data block related to this index entry
            // 2.2 verify data block header.
            DataBlockHeader headerReadout = DataBlockHeaderImpl.fromStream(dis);
            int expectedBlockSize = BlockAwareSegmentInputStreamImpl
                .calculateBlockSize(maxBlockSize, readHandle, entryIdTracker, entryBytesUploaded);
            Assert.assertEquals(headerReadout.getBlockLength(), expectedBlockSize);
            Assert.assertEquals(headerReadout.getFirstEntryId(), entryIdTracker);

            // 2.3 verify data block
            int entrySize = 0;
            long entryId = 0;
            for (int bytesReadout = headerReadout.getBlockLength() - DataBlockHeaderImpl.getDataStartOffset();
                bytesReadout > 0;
                bytesReadout -= (4 + 8 + entrySize)) {
                entrySize = dis.readInt();
                entryId = dis.readLong();
                byte[] bytes = new byte[(int) entrySize];
                dis.read(bytes);

                Assert.assertEquals(entrySize, entryLength);
                Assert.assertEquals(entryId, entryIdTracker ++);
                entryBytesUploaded += entrySize;
            }

            startPartIdTracker ++;
            startOffsetTracker += headerReadout.getBlockLength();
        }

        return;
    }

    @Test
    public void testHappyCase() throws Exception {
        LedgerOffloader offloader = new S3ManagedLedgerOffloader(s3client, BUCKET, scheduler, 1024);

        offloader.offload(buildReadHandle(1), UUID.randomUUID(), new HashMap<>()).get();
    }

    @Test
    public void testBucketDoesNotExist() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setManagedLedgerOffloadDriver(S3ManagedLedgerOffloader.DRIVER_NAME);
        conf.setS3ManagedLedgerOffloadBucket("no-bucket");
        conf.setS3ManagedLedgerOffloadServiceEndpoint(s3endpoint);
        conf.setS3ManagedLedgerOffloadRegion("eu-west-1");
        LedgerOffloader offloader = S3ManagedLedgerOffloader.create(conf, scheduler);

        try {
            offloader.offload(buildReadHandle(1), UUID.randomUUID(), new HashMap<>()).get();
            Assert.fail("Shouldn't be able to add to bucket");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getMessage().contains("NoSuchBucket"));
        }
    }

    @Test
    public void testNoRegionConfigured() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setManagedLedgerOffloadDriver(S3ManagedLedgerOffloader.DRIVER_NAME);
        conf.setS3ManagedLedgerOffloadBucket(BUCKET);

        try {
            S3ManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (PulsarServerException pse) {
            // correct
        }
    }

    @Test
    public void testNoBucketConfigured() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setManagedLedgerOffloadDriver(S3ManagedLedgerOffloader.DRIVER_NAME);
        conf.setS3ManagedLedgerOffloadRegion("eu-west-1");

        try {
            S3ManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (PulsarServerException pse) {
            // correct
        }
    }

    @Test
    public void testOffload() throws Exception {
        int entryLength = 10;
        int entryNumberEachBlock = 10;
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setManagedLedgerOffloadDriver(S3ManagedLedgerOffloader.DRIVER_NAME);

        conf.setS3ManagedLedgerOffloadBucket(BUCKET);
        conf.setS3ManagedLedgerOffloadRegion("eu-west-1");
        conf.setS3ManagedLedgerOffloadServiceEndpoint(s3endpoint);
        conf.setS3ManagedLedgerOffloadMaxBlockSizeInBytes(
            DataBlockHeaderImpl.getDataStartOffset() + (entryLength + 12) * entryNumberEachBlock);
        LedgerOffloader offloader = S3ManagedLedgerOffloader.create(conf, scheduler);

        // offload 30 entries, which will be placed into 3 data blocks.
        int entryCount = 30;
        ReadHandle readHandle = buildReadHandle(entryCount);
        UUID uuid = UUID.randomUUID();
        offloader.offload(readHandle, uuid, new HashMap<>()).get();

        S3Object obj = s3client.getObject(BUCKET, dataBlockOffloadKey(readHandle, uuid));
        S3Object indexObj = s3client.getObject(BUCKET, S3ManagedLedgerOffloader.indexBlockOffloadKey(readHandle, uuid));

        verifyS3ObjectRead(obj, indexObj, readHandle, 3, 30, conf.getS3ManagedLedgerOffloadMaxBlockSizeInBytes());
    }

    @Test
    public void testOffloadFailInitDataBlockUpload() throws Exception {
        int maxBlockSize = 1024;
        int entryCount = 3;
        ReadHandle readHandle = buildReadHandle(entryCount);
        UUID uuid = UUID.randomUUID();
        String failureString = "fail InitDataBlockUpload";

        // mock throw exception when initiateMultipartUpload
        try {
            AmazonS3 mockS3client = Mockito.spy(s3client);
            Mockito
                .doThrow(new AmazonServiceException(failureString))
                .when(mockS3client).initiateMultipartUpload(any());

            LedgerOffloader offloader = new S3ManagedLedgerOffloader(mockS3client, BUCKET, scheduler, maxBlockSize);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception when initiateMultipartUpload");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof AmazonServiceException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(s3client.doesObjectExist(BUCKET, dataBlockOffloadKey(readHandle, uuid)));
            Assert.assertFalse(s3client.doesObjectExist(BUCKET, indexBlockOffloadKey(readHandle, uuid)));
        }
    }

    @Test
    public void testOffloadFailDataBlockPartUpload() throws Exception {
        int maxBlockSize = 1024;
        int entryCount = 3;
        ReadHandle readHandle = buildReadHandle(entryCount);
        UUID uuid = UUID.randomUUID();
        String failureString = "fail DataBlockPartUpload";

        // mock throw exception when uploadPart
        try {
            AmazonS3 mockS3client = Mockito.spy(s3client);
            Mockito
                .doThrow(new AmazonServiceException("fail DataBlockPartUpload"))
                .when(mockS3client).uploadPart(any());
            Mockito.doNothing().when(mockS3client).abortMultipartUpload(any());

            LedgerOffloader offloader = new S3ManagedLedgerOffloader(mockS3client, BUCKET, scheduler, maxBlockSize);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception for when uploadPart");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof AmazonServiceException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(s3client.doesObjectExist(BUCKET, dataBlockOffloadKey(readHandle, uuid)));
            Assert.assertFalse(s3client.doesObjectExist(BUCKET, indexBlockOffloadKey(readHandle, uuid)));
        }
    }

    @Test
    public void testOffloadFailDataBlockUploadComplete() throws Exception {
        int maxBlockSize = 1024;
        int entryCount = 3;
        ReadHandle readHandle = buildReadHandle(entryCount);
        UUID uuid = UUID.randomUUID();
        String failureString = "fail DataBlockUploadComplete";

        // mock throw exception when completeMultipartUpload
        try {
            AmazonS3 mockS3client = Mockito.spy(s3client);
            Mockito
                .doThrow(new AmazonServiceException(failureString))
                .when(mockS3client).completeMultipartUpload(any());
            Mockito.doNothing().when(mockS3client).abortMultipartUpload(any());

            LedgerOffloader offloader = new S3ManagedLedgerOffloader(mockS3client, BUCKET, scheduler, maxBlockSize);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception for when completeMultipartUpload");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof AmazonServiceException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(s3client.doesObjectExist(BUCKET, dataBlockOffloadKey(readHandle, uuid)));
            Assert.assertFalse(s3client.doesObjectExist(BUCKET, indexBlockOffloadKey(readHandle, uuid)));
        }
    }

    @Test
    public void testOffloadFailPutIndexBlock() throws Exception {
        int maxBlockSize = 1024;
        int entryCount = 3;
        ReadHandle readHandle = buildReadHandle(entryCount);
        UUID uuid = UUID.randomUUID();
        String failureString = "fail putObject";

        // mock throw exception when putObject
        try {
            AmazonS3 mockS3client = Mockito.spy(s3client);
            Mockito
                .doThrow(new AmazonServiceException(failureString))
                .when(mockS3client).putObject(any());

            LedgerOffloader offloader = new S3ManagedLedgerOffloader(mockS3client, BUCKET, scheduler, maxBlockSize);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception for when putObject for index block");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof AmazonServiceException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(s3client.doesObjectExist(BUCKET, dataBlockOffloadKey(readHandle, uuid)));
            Assert.assertFalse(s3client.doesObjectExist(BUCKET, indexBlockOffloadKey(readHandle, uuid)));
        }
    }
}

