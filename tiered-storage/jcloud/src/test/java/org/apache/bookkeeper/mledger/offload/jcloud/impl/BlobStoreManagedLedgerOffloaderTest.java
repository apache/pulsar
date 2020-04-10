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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.BlobStoreTestBase;
import org.apache.bookkeeper.mledger.offload.jcloud.CredentialsUtil;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.jclouds.aws.domain.SessionCredentials;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.domain.Credentials;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class BlobStoreManagedLedgerOffloaderTest extends BlobStoreTestBase {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreManagedLedgerOffloaderTest.class);

    private static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<ACL>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(UTF_8), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(UTF_8), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }

    private static final int DEFAULT_BLOCK_SIZE = 5*1024*1024;
    private static final int DEFAULT_READ_BUFFER_SIZE = 1*1024*1024;
    final OrderedScheduler scheduler;
    final PulsarMockBookKeeper bk;

    public BlobStoreManagedLedgerOffloaderTest() throws Exception {
        scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("offloader").build();
        bk = new PulsarMockBookKeeper(createMockZooKeeper(), scheduler.chooseThread(this));
    }

    private ReadHandle buildReadHandle() throws Exception {
        return buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
    }

    private ReadHandle buildReadHandle(int maxBlockSize, int blockCount) throws Exception {
        Assert.assertTrue(maxBlockSize > DataBlockHeaderImpl.getDataStartOffset());

        LedgerHandle lh = bk.createLedger(1,1,1, BookKeeper.DigestType.CRC32, "foobar".getBytes());

        int i = 0;
        int bytesWrittenCurrentBlock = DataBlockHeaderImpl.getDataStartOffset();
        int blocksWritten = 1;
        int entries = 0;

        while (blocksWritten < blockCount
               || bytesWrittenCurrentBlock < maxBlockSize/2) {
            byte[] entry = ("foobar"+i).getBytes();
            int sizeInBlock = entry.length + 12 /* ENTRY_HEADER_SIZE */;

            if (bytesWrittenCurrentBlock + sizeInBlock > maxBlockSize) {
                bytesWrittenCurrentBlock = DataBlockHeaderImpl.getDataStartOffset();
                blocksWritten++;
                entries = 0;
            }
            entries++;

            lh.addEntry(entry);
            bytesWrittenCurrentBlock += sizeInBlock;
            i++;
        }

        lh.close();

        return bk.newOpenLedgerOp().withLedgerId(lh.getId())
            .withPassword("foobar".getBytes()).withDigestType(DigestType.CRC32).execute().get();
    }

    @Test
    public void testHappyCase() throws Exception {
        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, BUCKET, scheduler,
                DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
        offloader.offload(buildReadHandle(), UUID.randomUUID(), new HashMap<>()).get();
    }

    @Test
    public void testBucketDoesNotExist() throws Exception {
        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, "no-bucket", scheduler,
                                                                 DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
        try {
            offloader.offload(buildReadHandle(), UUID.randomUUID(), new HashMap<>()).get();
            Assert.fail("Shouldn't be able to add to bucket");
        } catch (ExecutionException e) {
            log.error("Exception: ", e);
            Assert.assertTrue(e.getMessage().toLowerCase().contains("not found"));
        }
    }

    @Test
    public void testNoRegionConfigured() throws Exception {
        OffloadPolicies conf = new OffloadPolicies();
        conf.setManagedLedgerOffloadDriver("s3");
        conf.setS3ManagedLedgerOffloadBucket(BUCKET);

        try {
            BlobStoreManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }

    @Test
    public void testNoBucketConfigured() throws Exception {
        OffloadPolicies conf = new OffloadPolicies();
        conf.setManagedLedgerOffloadDriver("s3");
        conf.setS3ManagedLedgerOffloadRegion("eu-west-1");

        try {
            BlobStoreManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }

    @Test
    public void testSmallBlockSizeConfigured() throws Exception {
        OffloadPolicies conf = new OffloadPolicies();
        conf.setManagedLedgerOffloadDriver("s3");
        conf.setS3ManagedLedgerOffloadRegion("eu-west-1");
        conf.setS3ManagedLedgerOffloadBucket(BUCKET);
        conf.setS3ManagedLedgerOffloadMaxBlockSizeInBytes(1024);

        try {
            BlobStoreManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
        }
    }

    @Test
    public void testGcsNoKeyPath() throws Exception {
        OffloadPolicies conf = new OffloadPolicies();
        conf.setManagedLedgerOffloadDriver("google-cloud-storage");
        conf.setGcsManagedLedgerOffloadBucket(BUCKET);

        try {
            BlobStoreManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
            log.error("Expected pse", pse);
        }
    }

    @Test
    public void testGcsNoBucketConfigured() throws Exception {
        OffloadPolicies conf = new OffloadPolicies();
        conf.setManagedLedgerOffloadDriver("google-cloud-storage");
        File tmpKeyFile = File.createTempFile("gcsOffload", "json");
        conf.setGcsManagedLedgerOffloadServiceAccountKeyFile(tmpKeyFile.getAbsolutePath());

        try {
            BlobStoreManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
            log.error("Expected pse", pse);
        }
    }

    @Test
    public void testGcsSmallBlockSizeConfigured() throws Exception {
        OffloadPolicies conf = new OffloadPolicies();
        conf.setManagedLedgerOffloadDriver("google-cloud-storage");
        File tmpKeyFile = File.createTempFile("gcsOffload", "json");
        conf.setGcsManagedLedgerOffloadServiceAccountKeyFile(tmpKeyFile.getAbsolutePath());
        conf.setGcsManagedLedgerOffloadBucket(BUCKET);
        conf.setGcsManagedLedgerOffloadMaxBlockSizeInBytes(1024);

        try {
            BlobStoreManagedLedgerOffloader.create(conf, scheduler);
            Assert.fail("Should have thrown exception");
        } catch (IOException pse) {
            // correct
            log.error("Expected pse", pse);
        }
    }

    @Test
    public void testS3DriverConfiguredWell() throws Exception {
        PowerMockito.mockStatic(CredentialsUtil.class);
        PowerMockito.when(CredentialsUtil.getAWSCredentialProvider(any())).thenReturn(new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new AWSSessionCredentials() {
                    @Override
                    public String getSessionToken() {
                        return "token";
                    }

                    @Override
                    public String getAWSAccessKeyId() {
                        return "access";
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return "secret";
                    }
                };
            }

            @Override
            public void refresh() {

            }
        });

        OffloadPolicies conf = new OffloadPolicies();
        conf.setManagedLedgerOffloadDriver("s3");
        conf.setS3ManagedLedgerOffloadBucket(BUCKET);
        conf.setS3ManagedLedgerOffloadServiceEndpoint("http://fake.s3.end.point");

        // should success and no exception thrown.
        BlobStoreManagedLedgerOffloader.create(conf, scheduler);
    }

    @Test
    public void testOffloadAndRead() throws Exception {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, BUCKET, scheduler,
                                                                 DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        try (LedgerEntries toWriteEntries = toWrite.read(0, toWrite.getLastAddConfirmed());
             LedgerEntries toTestEntries = toTest.read(0, toTest.getLastAddConfirmed())) {
            Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
            Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

            while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                LedgerEntry toWriteEntry = toWriteIter.next();
                LedgerEntry toTestEntry = toTestIter.next();

                Assert.assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                Assert.assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                Assert.assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
                Assert.assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
            }
            Assert.assertFalse(toWriteIter.hasNext());
            Assert.assertFalse(toTestIter.hasNext());
        }
    }

    @Test
    public void testOffloadFailInitDataBlockUpload() throws Exception {
        ReadHandle readHandle = buildReadHandle();
        UUID uuid = UUID.randomUUID();
        String failureString = "fail InitDataBlockUpload";

        // mock throw exception when initiateMultipartUpload
        try {

            BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
            Mockito
                .doThrow(new RuntimeException(failureString))
                .when(spiedBlobStore).initiateMultipartUpload(any(), any(), any());

            LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(spiedBlobStore, BUCKET, scheduler,
                                                                     DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception when initiateMultipartUpload");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadFailDataBlockPartUpload() throws Exception {
        ReadHandle readHandle = buildReadHandle();
        UUID uuid = UUID.randomUUID();
        String failureString = "fail DataBlockPartUpload";

        // mock throw exception when uploadPart
        try {
            BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
            Mockito
                .doThrow(new RuntimeException(failureString))
                .when(spiedBlobStore).uploadMultipartPart(any(), anyInt(), any());

            LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(spiedBlobStore, BUCKET, scheduler,
                DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception for when uploadPart");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadFailDataBlockUploadComplete() throws Exception {
        ReadHandle readHandle = buildReadHandle();
        UUID uuid = UUID.randomUUID();
        String failureString = "fail DataBlockUploadComplete";

        // mock throw exception when completeMultipartUpload
        try {
            BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
            Mockito
                .doThrow(new RuntimeException(failureString))
                .when(spiedBlobStore).completeMultipartUpload(any(), any());
            Mockito
                .doNothing()
                .when(spiedBlobStore).abortMultipartUpload(any());

            LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(spiedBlobStore, BUCKET, scheduler,
                DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();

            Assert.fail("Should throw exception for when completeMultipartUpload");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadFailPutIndexBlock() throws Exception {
        ReadHandle readHandle = buildReadHandle();
        UUID uuid = UUID.randomUUID();
        String failureString = "fail putObject";

        // mock throw exception when putObject
        try {
            BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
            Mockito
                .doThrow(new RuntimeException(failureString))
                .when(spiedBlobStore).putBlob(any(), any());

            LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(spiedBlobStore, BUCKET, scheduler,
                DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();

            Assert.fail("Should throw exception for when putObject for index block");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadReadRandomAccess() throws Exception {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        long[][] randomAccesses = new long[10][2];
        Random r = new Random(0);
        for (int i = 0; i < 10; i++) {
            long first = r.nextInt((int)toWrite.getLastAddConfirmed());
            long second = r.nextInt((int)toWrite.getLastAddConfirmed());
            if (second < first) {
                long tmp = first;
                first = second;
                second = tmp;
            }
            randomAccesses[i][0] = first;
            randomAccesses[i][1] = second;
        }

        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, BUCKET, scheduler,
                                                                 DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        for (long[] access : randomAccesses) {
            try (LedgerEntries toWriteEntries = toWrite.read(access[0], access[1]);
                 LedgerEntries toTestEntries = toTest.read(access[0], access[1])) {
                Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
                Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

                while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                    LedgerEntry toWriteEntry = toWriteIter.next();
                    LedgerEntry toTestEntry = toTestIter.next();

                    Assert.assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                    Assert.assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                    Assert.assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
                    Assert.assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
                }
                Assert.assertFalse(toWriteIter.hasNext());
                Assert.assertFalse(toTestIter.hasNext());
            }
        }
    }

    @Test
    public void testOffloadReadInvalidEntryIds() throws Exception {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, BUCKET, scheduler,
                                                                 DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        try {
            toTest.read(-1, -1);
            Assert.fail("Shouldn't be able to read anything");
        } catch (BKException.BKIncorrectParameterException e) {
        }

        try {
            toTest.read(0, toTest.getLastAddConfirmed() + 1);
            Assert.fail("Shouldn't be able to read anything");
        } catch (BKException.BKIncorrectParameterException e) {
        }
    }

    @Test
    public void testDeleteOffloaded() throws Exception {
        ReadHandle readHandle = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        UUID uuid = UUID.randomUUID();
        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, BUCKET, scheduler,
                                                                 DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);

        // verify object exist after offload
        offloader.offload(readHandle, uuid, new HashMap<>()).get();
        Assert.assertTrue(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(readHandle.getId(), uuid)));
        Assert.assertTrue(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(readHandle.getId(), uuid)));

        // verify object deleted after delete
        offloader.deleteOffloaded(readHandle.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(readHandle.getId(), uuid)));
        Assert.assertFalse(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(readHandle.getId(), uuid)));
    }

    @Test
    public void testDeleteOffloadedFail() throws Exception {
        String failureString = "fail deleteOffloaded";
        ReadHandle readHandle = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        UUID uuid = UUID.randomUUID();
        BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));

        Mockito
            .doThrow(new RuntimeException(failureString))
            .when(spiedBlobStore).removeBlobs(any(), any());

        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(spiedBlobStore, BUCKET, scheduler,
            DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);

        try {
            // verify object exist after offload
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.assertTrue(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertTrue(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(readHandle.getId(), uuid)));

            offloader.deleteOffloaded(readHandle.getId(), uuid, Collections.emptyMap()).get();
        } catch (Exception e) {
            // expected
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            // verify object still there.
            Assert.assertTrue(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertTrue(blobStore.blobExists(BUCKET, BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadEmpty() throws Exception {
        CompletableFuture<LedgerEntries> noEntries = new CompletableFuture<>();
        noEntries.completeExceptionally(new BKException.BKReadException());

        ReadHandle readHandle = Mockito.mock(ReadHandle.class);
        Mockito.doReturn(-1L).when(readHandle).getLastAddConfirmed();
        Mockito.doReturn(noEntries).when(readHandle).readAsync(anyLong(), anyLong());
        Mockito.doReturn(0L).when(readHandle).getLength();
        Mockito.doReturn(true).when(readHandle).isClosed();
        Mockito.doReturn(1234L).when(readHandle).getId();

        UUID uuid = UUID.randomUUID();
        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, BUCKET, scheduler,
                                                                 DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
        try {
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Shouldn't have been able to offload");
        } catch (ExecutionException e) {
            Assert.assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }

    @Test
    public void testReadUnknownDataVersion() throws Exception {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, BUCKET, scheduler,
                                                                 DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        String dataKey = BlobStoreManagedLedgerOffloader.dataBlockOffloadKey(toWrite.getId(), uuid);

        // Here it will return a Immutable map.
        Map<String, String> immutableMap = blobStore.blobMetadata(BUCKET, dataKey).getUserMetadata();
        Map<String, String> userMeta = Maps.newHashMap();
        userMeta.putAll(immutableMap);
        userMeta.put(BlobStoreManagedLedgerOffloader.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(-12345));
        blobStore.copyBlob(BUCKET, dataKey, BUCKET, dataKey, CopyOptions.builder().userMetadata(userMeta).build());

        try (ReadHandle toRead = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get()) {
            toRead.readAsync(0, 0).get();
            Assert.fail("Shouldn't have been able to read");
        } catch (ExecutionException e) {
            log.error("Exception: ", e);
            Assert.assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Error reading from BlobStore"));
        }

        userMeta.put(BlobStoreManagedLedgerOffloader.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(12345));
        blobStore.copyBlob(BUCKET, dataKey, BUCKET, dataKey, CopyOptions.builder().userMetadata(userMeta).build());

        try (ReadHandle toRead = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get()) {
            toRead.readAsync(0, 0).get();
            Assert.fail("Shouldn't have been able to read");
        } catch (ExecutionException e) {
            Assert.assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Error reading from BlobStore"));
        }
    }

    @Test
    public void testReadUnknownIndexVersion() throws Exception {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        LedgerOffloader offloader = new BlobStoreManagedLedgerOffloader(blobStore, BUCKET, scheduler,
                                                                 DEFAULT_BLOCK_SIZE, DEFAULT_READ_BUFFER_SIZE);
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        String indexKey = BlobStoreManagedLedgerOffloader.indexBlockOffloadKey(toWrite.getId(), uuid);

        // Here it will return a Immutable map.
        Map<String, String> immutableMap = blobStore.blobMetadata(BUCKET, indexKey).getUserMetadata();
        Map<String, String> userMeta = Maps.newHashMap();
        userMeta.putAll(immutableMap);
        userMeta.put(BlobStoreManagedLedgerOffloader.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(-12345));
        blobStore.copyBlob(BUCKET, indexKey, BUCKET, indexKey, CopyOptions.builder().userMetadata(userMeta).build());

        try {
            offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
            Assert.fail("Shouldn't have been able to open");
        } catch (ExecutionException e) {
            Assert.assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid object version"));
        }

        userMeta.put(BlobStoreManagedLedgerOffloader.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(12345));
        blobStore.copyBlob(BUCKET, indexKey, BUCKET, indexKey, CopyOptions.builder().userMetadata(userMeta).build());

        try {
            offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
            Assert.fail("Shouldn't have been able to open");
        } catch (ExecutionException e) {
            Assert.assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid object version"));
        }
    }

    @Test
    public void testSessionCredentialSupplier() throws Exception {
        PowerMockito.mockStatic(CredentialsUtil.class);
        PowerMockito.when(CredentialsUtil.getAWSCredentialProvider(any())).thenReturn(new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new AWSSessionCredentials() {
                    @Override
                    public String getSessionToken() {
                        return "token";
                    }

                    @Override
                    public String getAWSAccessKeyId() {
                        return "access";
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return "secret";
                    }
                };
            }

            @Override
            public void refresh() {

            }
        });

        Supplier<Credentials> creds = BlobStoreManagedLedgerOffloader.getCredentials("aws-s3", any());

        Assert.assertTrue(creds.get() instanceof SessionCredentials);
        SessionCredentials sessCreds = (SessionCredentials) creds.get();
        Assert.assertEquals(sessCreds.getAccessKeyId(), "access");
        Assert.assertEquals(sessCreds.getSecretAccessKey(), "secret");
        Assert.assertEquals(sessCreds.getSessionToken(), "token");
    }
}

