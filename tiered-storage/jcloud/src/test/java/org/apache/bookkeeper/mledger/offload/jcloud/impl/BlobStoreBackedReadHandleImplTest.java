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

import org.apache.bookkeeper.client.api.ReadHandle;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.Payload;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.*;

public class BlobStoreBackedReadHandleImplTest {

    private final static String bucket = "test-offload-read";
    private final static int firstLedgerID = 1;
    private final static String firstLedgerKey = "ledger-1";
    private final static String firstLedgerIndexKey = "ledger-1-index";
    private final static int secondLedgerID = 2;
    private final static String secondLedgerKey = "ledger-2";
    private final static String secondLedgerIndexKey = "ledger-2-index";

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    DataBlockUtils.VersionCheck versionCheck = (k, b) -> { return; };
    private final static int readBufferSize = 1024;

    @Test
    public void testRead() throws Exception {
        InputStream firstIndexInputStream = getClass().getClassLoader().getResourceAsStream(firstLedgerIndexKey);
        InputStream firstDataInputStream = getClass().getClassLoader().getResourceAsStream(firstLedgerKey);
        InputStream secondIndexInputStream = getClass().getClassLoader().getResourceAsStream(secondLedgerIndexKey);
        InputStream secondDataInputStream = getClass().getClassLoader().getResourceAsStream(secondLedgerKey);

        BlobStore blobStore = mock(BlobStore.class);

        Blob firstIndexBlob = mock(Blob.class);
        Payload firstIndexPayload = mock(Payload.class);
        when(blobStore.getBlob(eq(bucket), eq(firstLedgerIndexKey))).thenReturn(firstIndexBlob);
        when(firstIndexBlob.getPayload()).thenReturn(firstIndexPayload);
        when(firstIndexPayload.openStream()).thenReturn(firstIndexInputStream);

        Blob firstDataBlob = mock(Blob.class);
        Payload firstDataPayload = mock(Payload.class);
        when(blobStore.getBlob(eq(bucket), eq(firstLedgerKey), any())).thenReturn(firstDataBlob);
        when(firstDataBlob.getPayload()).thenReturn(firstDataPayload);
        when(firstDataPayload.openStream()).thenReturn(firstDataInputStream);

        Blob secondIndexBlob = mock(Blob.class);
        Payload secondIndexPayload = mock(Payload.class);
        when(blobStore.getBlob(eq(bucket), eq(secondLedgerIndexKey))).thenReturn(secondIndexBlob);
        when(secondIndexBlob.getPayload()).thenReturn(secondIndexPayload);
        when(secondIndexPayload.openStream()).thenReturn(secondIndexInputStream);

        Blob secondDataBlob = mock(Blob.class);
        Payload secondDataPayload = mock(Payload.class);
        when(blobStore.getBlob(eq(bucket), eq(secondLedgerKey), any())).thenReturn(secondDataBlob);
        when(secondDataBlob.getPayload()).thenReturn(secondDataPayload);
        when(secondDataPayload.openStream()).thenReturn(secondDataInputStream);

        CountDownLatch latch = new CountDownLatch(3);
        ReadHandle firstRead = BlobStoreBackedReadHandleImpl.open(executorService, blobStore,
            bucket, firstLedgerKey, firstLedgerIndexKey, versionCheck, firstLedgerID, readBufferSize);
        firstRead.readAsync(0, 0).whenComplete((ledgerEntries, throwable) -> {
            if (throwable == null) {
                latch.countDown();
            }
        });
        firstRead.closeAsync();
        firstRead.readAsync(0, 0).whenComplete((ledgerEntries, throwable) -> {
            if (throwable != null) {
                latch.countDown();
            }
        });

        ReadHandle secondRead = BlobStoreBackedReadHandleImpl.open(executorService, blobStore,
            bucket, secondLedgerKey, secondLedgerIndexKey, versionCheck, secondLedgerID, readBufferSize);
        firstRead.readAsync(0, 0).whenComplete((ledgerEntries, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
                latch.countDown();
            }
        });
        secondRead.close();
        latch.await();
    }
}
