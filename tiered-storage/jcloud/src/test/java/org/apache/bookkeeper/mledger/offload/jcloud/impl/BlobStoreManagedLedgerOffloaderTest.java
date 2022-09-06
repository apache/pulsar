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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.options.CopyOptions;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class BlobStoreManagedLedgerOffloaderTest extends BlobStoreManagedLedgerOffloaderBase {

    private static final Logger log = LoggerFactory.getLogger(BlobStoreManagedLedgerOffloaderTest.class);
    private TieredStorageConfiguration mockedConfig;

    BlobStoreManagedLedgerOffloaderTest() throws Exception {
        super();
        config = getConfiguration(BUCKET);
        JCloudBlobStoreProvider provider = getBlobStoreProvider();
        assertNotNull(provider);
        provider.validate(config);
        blobStore = provider.getBlobStore(config);
    }

    private BlobStoreManagedLedgerOffloader getOffloader() throws IOException {
        return getOffloader(BUCKET);
    }
    
    private BlobStoreManagedLedgerOffloader getOffloader(BlobStore mockedBlobStore) throws IOException {
        return getOffloader(BUCKET, mockedBlobStore);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(String bucket) throws IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(bucket)));
        Mockito.doReturn(blobStore).when(mockedConfig).getBlobStore(); // Use the REAL blobStore
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader.create(mockedConfig, new HashMap<String,String>(), scheduler);
        return offloader;
    }
    
    private BlobStoreManagedLedgerOffloader getOffloader(String bucket, BlobStore mockedBlobStore) throws IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(bucket)));
        Mockito.doReturn(mockedBlobStore).when(mockedConfig).getBlobStore(); 
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader.create(mockedConfig, new HashMap<String,String>(), scheduler);
        return offloader;
    }

    @Test(timeOut = 600000)  // 10 minutes.
    public void testHappyCase() throws Exception {
        LedgerOffloader offloader = getOffloader();
        offloader.offload(buildReadHandle(), UUID.randomUUID(), new HashMap<>()).get();
    }

    @Test(timeOut = 600000)  // 10 minutes.
    public void testBucketDoesNotExist() throws Exception {

        if (provider == JCloudBlobStoreProvider.TRANSIENT) {
            // Skip this test, since it isn't applicable.
            return;
        }

        LedgerOffloader offloader = getOffloader("some-non-existant-bucket-name");
        try {
            offloader.offload(buildReadHandle(), UUID.randomUUID(), new HashMap<>()).get();
            Assert.fail("Shouldn't be able to add to bucket");
        } catch (ExecutionException e) {
            log.error("Exception: ", e);
            Assert.assertTrue(e.getMessage().toLowerCase().contains("not found"));
        }
    }

    @Test(timeOut = 600000)  // 10 minutes.
    public void testOffloadAndRead() throws Exception {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        LedgerOffloader offloader = getOffloader();

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

    @Test(timeOut = 60000)
    public void testReadHandlerState() throws Exception {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        LedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        BlobStoreBackedReadHandleImpl toTest = (BlobStoreBackedReadHandleImpl) offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());
        Assert.assertEquals(toTest.getState(), BlobStoreBackedReadHandleImpl.State.Opened);
        toTest.read(0, 1);
        toTest.close();
        Assert.assertEquals(toTest.getState(), BlobStoreBackedReadHandleImpl.State.Closed);
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

            BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception when initiateMultipartUpload");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
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

            BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception for when uploadPart");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
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

            BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();           

            Assert.fail("Should throw exception for when completeMultipartUpload");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
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

            BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();

            Assert.fail("Should throw exception for when putObject for index block");
         } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test(timeOut = 600000)
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

        LedgerOffloader offloader = getOffloader();

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
        LedgerOffloader offloader = getOffloader();
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
        
        BlobStoreManagedLedgerOffloader offloader = getOffloader();

        // verify object exist after offload
        offloader.offload(readHandle, uuid, new HashMap<>()).get();
        Assert.assertTrue(blobStore.blobExists(BUCKET, DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
        Assert.assertTrue(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));

        // verify object deleted after delete
        offloader.deleteOffloaded(readHandle.getId(), uuid, config.getOffloadDriverMetadata()).get();
        Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
        Assert.assertFalse(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
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
        
        BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);

        try {
            // verify object exist after offload
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.assertTrue(blobStore.blobExists(BUCKET, DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertTrue(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));

            offloader.deleteOffloaded(readHandle.getId(), uuid, config.getOffloadDriverMetadata()).get();
        } catch (Exception e) {
            // expected
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            // verify object still there.
            Assert.assertTrue(blobStore.blobExists(BUCKET, DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertTrue(blobStore.blobExists(BUCKET, DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
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
        LedgerOffloader offloader = getOffloader();

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
        BlobStoreManagedLedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        String dataKey = DataBlockUtils.dataBlockOffloadKey(toWrite.getId(), uuid);

        // Here it will return a Immutable map.
        Assert.assertTrue(blobStore.blobExists(BUCKET, dataKey));
        Map<String, String> immutableMap = blobStore.blobMetadata(BUCKET, dataKey).getUserMetadata();
        Map<String, String> userMeta = Maps.newHashMap();
        userMeta.putAll(immutableMap);
        userMeta.put(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(-12345));
        blobStore.copyBlob(BUCKET, dataKey, BUCKET, dataKey, CopyOptions.builder().userMetadata(userMeta).build());

        try (ReadHandle toRead = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get()) {
            toRead.readAsync(0, 0).get();
            Assert.fail("Shouldn't have been able to read");
        } catch (ExecutionException e) {
            log.error("Exception: ", e);
            Assert.assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Error reading from BlobStore"));
        }

        userMeta.put(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(12345));
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
        BlobStoreManagedLedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        String indexKey = DataBlockUtils.indexBlockOffloadKey(toWrite.getId(), uuid);

        // Here it will return a Immutable map.
        Map<String, String> immutableMap = blobStore.blobMetadata(BUCKET, indexKey).getUserMetadata();
        Map<String, String> userMeta = Maps.newHashMap();
        userMeta.putAll(immutableMap);
        userMeta.put(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(-12345));
        blobStore.copyBlob(BUCKET, indexKey, BUCKET, indexKey, CopyOptions.builder().userMetadata(userMeta).build());

        try {
            offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
            Assert.fail("Shouldn't have been able to open");
        } catch (ExecutionException e) {
            Assert.assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid object version"));
        }

        userMeta.put(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(12345));
        blobStore.copyBlob(BUCKET, indexKey, BUCKET, indexKey, CopyOptions.builder().userMetadata(userMeta).build());

        try {
            offloader.readOffloaded(toWrite.getId(), uuid, config.getOffloadDriverMetadata()).get();
            Assert.fail("Shouldn't have been able to open");
        } catch (ExecutionException e) {
            Assert.assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid object version"));
        }
    }

    @Test
    public void testReadEOFException() throws Throwable {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        LedgerOffloader offloader = getOffloader();
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());
        toTest.readAsync(0, toTest.getLastAddConfirmed()).get();

        try {
            toTest.readAsync(0, 0).get();
        } catch (Exception e) {
            fail("Get unexpected exception when reading entries", e);
        }
    }

    @Test
    public void testReadWithAClosedLedgerHandler() throws Exception {
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        LedgerOffloader offloader = getOffloader();
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());
        long lac = toTest.getLastAddConfirmed();
        toTest.readAsync(0, lac).get();
        toTest.closeAsync().get();
        try {
            toTest.readAsync(0, lac).get();
        } catch (Exception e) {
            if (e.getCause() instanceof ManagedLedgerException.OffloadReadHandleClosedException) {
                // expected exception
                return;
            }
            throw e;
        }
    }
}
