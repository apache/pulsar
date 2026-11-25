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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.awaitility.Awaitility;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BlobStoreStorageCheckTest {
    private static final String BUCKET = "offload-storage-check";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private OrderedScheduler scheduler;
    private BlobStoreContext realContext;
    private BlobStoreContext checkContext;
    private BlobStore store;
    private TieredStorageConfiguration checkConfig;
    private BlobStoreManagedLedgerOffloader offloader;

    @BeforeMethod
    public void setup() {
        scheduler = OrderedScheduler.newSchedulerBuilder().name("storage-check-test").numThreads(1).build();
        realContext = ContextBuilder.newBuilder("transient").buildView(BlobStoreContext.class);
        checkContext = mock(BlobStoreContext.class, delegatesTo(realContext));
        store = mock(BlobStore.class, delegatesTo(realContext.getBlobStore()));
        doReturn(checkContext).when(store).getContext();
        store.createContainerInLocation(null, BUCKET);
        TieredStorageConfiguration config = mock(TieredStorageConfiguration.class,
                delegatesTo(TieredStorageConfiguration.create(Map.of(
                        TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "transient",
                        "managedLedgerOffloadBucket", BUCKET))));
        checkConfig = mock(TieredStorageConfiguration.class);
        doReturn(store).when(checkConfig).getBlobStore();
        doReturn(checkConfig).when(config).forStorageCheck(any());
        offloader = new BlobStoreManagedLedgerOffloader(config, scheduler, scheduler, Map.of(),
                LedgerOffloaderStats.create(false, false, scheduler, 60), mock(OffsetsCache.class));
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        }
        if (realContext != null) {
            realContext.close();
        }
    }

    @Test
    public void testSuccessfulCheckPreservesExistingObjects() throws Exception {
        BlobStore realStore = realContext.getBlobStore();
        realStore.putBlob(BUCKET, realStore.blobBuilder("existing-ledger").payload("data").build());

        offloader.checkStorageAsync(TIMEOUT).get(10, TimeUnit.SECONDS);

        assertEquals(realStore.countBlobs(BUCKET), 1);
        assertTrue(realStore.blobExists(BUCKET, "existing-ledger"));
        verify(store).removeBlob(eq(BUCKET), anyString());
        verify(checkContext).close();
    }

    @Test
    public void testMissingBucket() {
        store.deleteContainer(BUCKET);

        ExecutionException failure = expectThrows(ExecutionException.class,
                () -> offloader.checkStorageAsync(TIMEOUT).get(10, TimeUnit.SECONDS));

        assertThat(failure.getCause()).isInstanceOf(IOException.class).hasMessageContaining(BUCKET);
        verify(store, never()).putBlob(anyString(), any(Blob.class));
        verify(checkContext).close();
    }

    @Test
    public void testWritePermissionFailure() {
        RuntimeException denied = new RuntimeException("Write access denied");
        doThrow(denied).when(store).putBlob(eq(BUCKET), any(Blob.class));

        ExecutionException failure = expectThrows(ExecutionException.class,
                () -> offloader.checkStorageAsync(TIMEOUT).get(10, TimeUnit.SECONDS));

        assertThat(failure.getCause()).isSameAs(denied);
        verify(store).removeBlob(eq(BUCKET), anyString());
        verify(checkContext).close();
    }

    @DataProvider
    public Object[][] cleanupFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "cleanupFailures")
    public void testCleanupAfterUploadLosesResponse(boolean cleanupFails) {
        RuntimeException uploadFailure = new RuntimeException("Connection lost after storing the object");
        RuntimeException cleanupFailure = new RuntimeException("Delete access denied");
        doAnswer(invocation -> {
            realContext.getBlobStore().putBlob(BUCKET, invocation.getArgument(1));
            throw uploadFailure;
        }).when(store).putBlob(eq(BUCKET), any(Blob.class));
        if (cleanupFails) {
            doThrow(cleanupFailure).when(store).removeBlob(eq(BUCKET), anyString());
        }

        ExecutionException failure = expectThrows(ExecutionException.class,
                () -> offloader.checkStorageAsync(TIMEOUT).get(10, TimeUnit.SECONDS));

        assertThat(failure.getCause()).isSameAs(uploadFailure);
        if (cleanupFails) {
            assertThat(uploadFailure.getSuppressed()).containsExactly(cleanupFailure);
        } else {
            assertEquals(realContext.getBlobStore().countBlobs(BUCKET), 0);
        }
        verify(store).removeBlob(eq(BUCKET), anyString());
        verify(checkContext).close();
    }

    @Test
    public void testDeleteFailureIsReported() {
        RuntimeException denied = new RuntimeException("Delete access denied");
        doThrow(denied).when(store).removeBlob(eq(BUCKET), anyString());

        ExecutionException failure = expectThrows(ExecutionException.class,
                () -> offloader.checkStorageAsync(TIMEOUT).get(10, TimeUnit.SECONDS));

        assertThat(failure.getCause()).isSameAs(denied);
        verify(checkContext).close();
    }

    @Test(timeOut = 30000)
    public void testCleanupContinuesAfterCallerTimesOut() throws Exception {
        CountDownLatch uploadStarted = new CountDownLatch(1);
        CountDownLatch releaseUpload = new CountDownLatch(1);
        doAnswer(invocation -> {
            uploadStarted.countDown();
            if (!releaseUpload.await(20, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Upload was not released");
            }
            return realContext.getBlobStore().putBlob(BUCKET, invocation.getArgument(1));
        }).when(store).putBlob(eq(BUCKET), any(Blob.class));
        try {
            CompletableFuture<Void> check = offloader.checkStorageAsync(TIMEOUT);
            assertTrue(uploadStarted.await(10, TimeUnit.SECONDS));
            check.orTimeout(1, TimeUnit.MILLISECONDS);
            ExecutionException failure = expectThrows(ExecutionException.class,
                    () -> check.get(10, TimeUnit.SECONDS));
            assertThat(failure.getCause()).isInstanceOf(TimeoutException.class);
        } finally {
            releaseUpload.countDown();
        }

        Awaitility.await().untilAsserted(() -> verify(checkContext).close());
        assertEquals(realContext.getBlobStore().countBlobs(BUCKET), 0);
    }

    @Test
    public void testClientCreationFailureIsReportedAsynchronously() {
        RuntimeException failure = new RuntimeException("Invalid credentials");
        doThrow(failure).when(checkConfig).getBlobStore();

        ExecutionException result = expectThrows(ExecutionException.class,
                () -> offloader.checkStorageAsync(TIMEOUT).get(10, TimeUnit.SECONDS));

        assertThat(result.getCause()).isSameAs(failure);
    }

    @Test
    public void testRejectedCheckReturnsFailedFuture() {
        scheduler.shutdown();

        ExecutionException failure = expectThrows(ExecutionException.class,
                () -> offloader.checkStorageAsync(TIMEOUT).get(10, TimeUnit.SECONDS));

        assertThat(failure.getCause()).isInstanceOf(RejectedExecutionException.class);
    }
}
