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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.offload.jcloud.BlobStoreTestBase;
import org.awaitility.Awaitility;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.testng.annotations.Test;

public class OffloadedDataChunkCacheTest extends BlobStoreTestBase {
    private static final OffloadedDataChunkCache.Key KEY =
            new OffloadedDataChunkCache.Key("bucket", "object", 0, 16);

    private static ByteBuf bytes(OffloadedDataChunkCache.Key key) {
        ByteBuf buffer = Unpooled.buffer(key.length());
        for (int i = 0; i < key.length(); i++) {
            buffer.writeByte((int) (key.start() + i));
        }
        return buffer;
    }

    @Test
    public void readsOfAChunkThatIsBeingFetchedJoinTheFetch() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1024, 60, 0, 2);
        CountDownLatch fetching = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger fetches = new AtomicInteger();
        OffloadedDataChunkCache.Fetcher fetcher = key -> {
            fetches.incrementAndGet();
            fetching.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            return bytes(key);
        };
        @Cleanup("shutdownNow")
        ExecutorService readers = Executors.newFixedThreadPool(8);
        List<Future<OffloadedDataChunkCache.Chunk>> reads = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            reads.add(readers.submit(() -> cache.acquire(KEY, fetcher)));
        }
        assertThat(fetching.await(10, TimeUnit.SECONDS)).isTrue();
        // every read waits for the fetch before it completes
        Awaitility.await().untilAsserted(() -> assertThat(cache.waiters(KEY)).isEqualTo(8));
        release.countDown();
        OffloadedDataChunkCache.Chunk first = reads.get(0).get(10, TimeUnit.SECONDS);
        for (Future<OffloadedDataChunkCache.Chunk> read : reads) {
            assertThat(read.get(10, TimeUnit.SECONDS)).isSameAs(first);
        }
        assertThat(fetches.get()).isEqualTo(1);
        assertThat(cache.fetches()).isEqualTo(1);
        // the cache's reference and one for each read
        assertThat(first.references()).isEqualTo(9);
        for (Future<OffloadedDataChunkCache.Chunk> read : reads) {
            read.get().release();
        }
        assertThat(first.references()).isEqualTo(1);
        assertThat(first.buffer().getByte(3)).isEqualTo((byte) 3);
    }

    @Test
    public void keepsFetchedChunksWithinTheSizeBudget() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(32, 60, 0, 1);
        AtomicInteger fetches = new AtomicInteger();
        OffloadedDataChunkCache.Fetcher fetcher = key -> {
            fetches.incrementAndGet();
            return bytes(key);
        };
        OffloadedDataChunkCache.Chunk chunk = cache.acquire(KEY, fetcher);
        chunk.release();
        OffloadedDataChunkCache.Chunk again = cache.acquire(KEY, fetcher);
        assertThat(again).isSameAs(chunk);
        assertThat(fetches.get()).isEqualTo(1);
        assertThat(cache.hits()).isEqualTo(1);
        // three chunks of 16 bytes don't fit into 32: the least recently used is dropped, but the read that holds it
        // keeps it readable until it releases it
        for (int i = 1; i <= 2; i++) {
            cache.acquire(new OffloadedDataChunkCache.Key("bucket", "object", 16L * i, 16), fetcher).release();
        }
        assertThat(cache.cachedBytes()).isEqualTo(32);
        assertThat(again.references()).isEqualTo(1);
        assertThat(again.buffer().getByte(15)).isEqualTo((byte) 15);
        again.release();
        assertThat(again.buffer().refCnt()).isZero();
        // the two later chunks are still cached
        cache.acquire(new OffloadedDataChunkCache.Key("bucket", "object", 32, 16), fetcher).release();
        assertThat(fetches.get()).isEqualTo(3);
    }

    @Test
    public void releasesTheBufferOfAnEvictedChunkWhenItsLastReadReleasesIt() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1024, 60, 0, 1);
        OffloadedDataChunkCache.Chunk chunk = cache.acquire(KEY, OffloadedDataChunkCacheTest::bytes);
        cache.clear();
        assertThat(chunk.buffer().refCnt()).isEqualTo(1);
        chunk.release();
        assertThat(chunk.buffer().refCnt()).isZero();
        // dropped from the cache, so fetched again
        cache.acquire(KEY, OffloadedDataChunkCacheTest::bytes).release();
        assertThat(cache.fetches()).isEqualTo(2);
    }

    @Test
    public void releasesTheBuffersOfExpiredChunks() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1024, 1, 0, 1);
        OffloadedDataChunkCache.Chunk chunk = cache.acquire(KEY, OffloadedDataChunkCacheTest::bytes);
        chunk.release();
        assertThat(chunk.references()).isEqualTo(1);
        // the cleanup drops it once no read has used it for the expiry
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(chunk.buffer().refCnt()).isZero());
        assertThat(cache.cachedBytes()).isZero();
    }

    @Test
    public void anInterruptedReadReleasesItsReferenceWhenTheFetchCompletes() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1024, 60, 0, 1);
        CountDownLatch release = new CountDownLatch(1);
        OffloadedDataChunkCache.Fetcher fetcher = key -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            return bytes(key);
        };
        @Cleanup("shutdownNow")
        ExecutorService readers = Executors.newFixedThreadPool(2);
        // the first read fetches on its thread, the second waits for that fetch
        Future<OffloadedDataChunkCache.Chunk> fetching = readers.submit(() -> cache.acquire(KEY, fetcher));
        Awaitility.await().untilAsserted(() -> assertThat(cache.waiters(KEY)).isEqualTo(1));
        Future<OffloadedDataChunkCache.Chunk> waiting = readers.submit(() -> cache.acquire(KEY, fetcher));
        Awaitility.await().untilAsserted(() -> assertThat(cache.waiters(KEY)).isEqualTo(2));
        waiting.cancel(true);
        release.countDown();
        OffloadedDataChunkCache.Chunk chunk = fetching.get(10, TimeUnit.SECONDS);
        // the cache's reference and the first read's: the interrupted read's has been released
        Awaitility.await().untilAsserted(() -> assertThat(chunk.references()).isEqualTo(2));
        chunk.release();
        assertThat(cache.fetches()).isEqualTo(1);
    }

    @Test
    public void aReadThatStartsAFetchGetsItsChunkWithoutABudget() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(0, 60, 0, 1);
        // the read is the fetch's waiter from the start, so it can't miss the chunk that nothing else keeps
        for (int i = 1; i <= 100; i++) {
            OffloadedDataChunkCache.Chunk chunk = cache.acquire(KEY, OffloadedDataChunkCacheTest::bytes);
            assertThat(chunk.references()).isEqualTo(1);
            chunk.release();
            assertThat(cache.fetches()).isEqualTo(i);
        }
        assertThat(cache.liveChunks()).isZero();
    }

    @Test
    public void prefetchesAtMostAsManyChunksAtATimeAsThereAreThreads() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1 << 20, 60, 1, 2);
        CountDownLatch release = new CountDownLatch(1);
        OffloadedDataChunkCache.Fetcher blocked = key -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            return bytes(key);
        };
        for (int i = 0; i < 5; i++) {
            cache.prefetch(new OffloadedDataChunkCache.Key("bucket", "object", 16L * i, 16), blocked);
        }
        Awaitility.await().untilAsserted(() -> assertThat(cache.fetches()).isEqualTo(2));
        // the others were skipped, not queued
        assertThat(cache.waiters(new OffloadedDataChunkCache.Key("bucket", "object", 32, 16))).isEqualTo(-1);
        release.countDown();
        Awaitility.await().untilAsserted(() -> assertThat(cache.cachedBytes()).isEqualTo(32));
        cache.prefetch(new OffloadedDataChunkCache.Key("bucket", "object", 32, 16), blocked);
        Awaitility.await().untilAsserted(() -> assertThat(cache.cachedBytes()).isEqualTo(48));
    }

    @Test
    public void doesNotPrefetchAChunkLargerThanTheBudget() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(8, 60, 1, 1);
        cache.prefetch(KEY, OffloadedDataChunkCacheTest::bytes);
        assertThat(cache.fetches()).isZero();
    }

    @Test
    public void withoutABudgetKeepsNoChunk() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(0, 60, 1, 1);
        OffloadedDataChunkCache.Chunk chunk = cache.acquire(KEY, OffloadedDataChunkCacheTest::bytes);
        assertThat(chunk.references()).isEqualTo(1);
        chunk.release();
        assertThat(chunk.buffer().refCnt()).isZero();
        cache.acquire(KEY, OffloadedDataChunkCacheTest::bytes).release();
        assertThat(cache.fetches()).isEqualTo(2);
        // nothing would keep a prefetched chunk
        assertThat(cache.prefetchChunks()).isZero();
    }

    @Test
    public void failsTheReadsOfAFailedFetchAndFetchesAgainLater() throws Exception {
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1024, 60, 0, 1);
        assertThatThrownBy(() -> cache.acquire(KEY, key -> {
            throw new IOException("unavailable");
        })).isInstanceOf(IOException.class).hasMessage("unavailable");
        OffloadedDataChunkCache.Chunk chunk = cache.acquire(KEY, OffloadedDataChunkCacheTest::bytes);
        assertThat(chunk.buffer().getByte(1)).isEqualTo((byte) 1);
        chunk.release();
        assertThat(cache.fetches()).isEqualTo(2);
    }

    @Test
    public void closingFailsTheReadsThatWaitForAFetch() throws Exception {
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1024, 60, 0, 1);
        CountDownLatch release = new CountDownLatch(1);
        OffloadedDataChunkCache.Fetcher fetcher = key -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            return bytes(key);
        };
        @Cleanup("shutdownNow")
        ExecutorService readers = Executors.newFixedThreadPool(2);
        Future<OffloadedDataChunkCache.Chunk> fetching = readers.submit(() -> cache.acquire(KEY, fetcher));
        Awaitility.await().untilAsserted(() -> assertThat(cache.waiters(KEY)).isEqualTo(1));
        Future<OffloadedDataChunkCache.Chunk> waiting = readers.submit(() -> cache.acquire(KEY, fetcher));
        Awaitility.await().untilAsserted(() -> assertThat(cache.waiters(KEY)).isEqualTo(2));
        cache.close();
        // both fail at once, without waiting for the fetch
        assertThatThrownBy(() -> waiting.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(IOException.class);
        assertThatThrownBy(() -> fetching.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
        release.countDown();
        assertThatThrownBy(() -> fetching.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(IOException.class);
        // the chunk that the fetch completed after the close is released
        Awaitility.await().untilAsserted(() -> assertThat(cache.liveChunks()).isZero());
    }

    @Test
    public void aClosedStreamDoesNotRead() throws Exception {
        String objectKey = "closedStream";
        byte[] data = new byte[2_000];
        blobStore.putBlob(BUCKET, blobStore.blobBuilder(objectKey).payload(data).contentLength(data.length).build());
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1 << 20, 60, 0, 1);
        BlobStoreBackedInputStreamImpl stream = new BlobStoreBackedInputStreamImpl(blobStore, BUCKET, objectKey,
                (key, md) -> {}, data.length, 1000, cache);
        stream.read();
        stream.close();
        stream.close();
        stream.seek(1500);
        assertThatThrownBy(stream::read).isInstanceOf(IOException.class).hasMessageContaining("closed");
        // only the cache holds the chunk that the stream read
        cache.clear();
        assertThat(cache.liveChunks()).isZero();
    }

    @Test
    public void failsAReadOfATruncatedRange() throws Exception {
        String objectKey = "truncated";
        byte[] data = new byte[500];
        blobStore.putBlob(BUCKET, blobStore.blobBuilder(objectKey).payload(data).contentLength(data.length).build());
        // the stream expects 1,000 bytes, of which the store has 500
        @Cleanup
        BlobStoreBackedInputStreamImpl stream = new BlobStoreBackedInputStreamImpl(blobStore, BUCKET, objectKey,
                (key, md) -> {}, 1_000, 1000);
        assertThatThrownBy(stream::read).isInstanceOf(IOException.class);
    }

    @Test
    public void streamsShareTheChunksOfAnObject() throws Exception {
        String objectKey = "sharedChunks";
        int objectSize = 10_000;
        byte[] data = new byte[objectSize];
        new Random(1).nextBytes(data);
        Blob blob = blobStore.blobBuilder(objectKey).payload(data).contentLength(objectSize).build();
        blobStore.putBlob(BUCKET, blob);
        BlobStore countingBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1 << 20, 60, 0, 2);
        @Cleanup
        BlobStoreBackedInputStreamImpl first = new BlobStoreBackedInputStreamImpl(countingBlobStore, BUCKET,
                objectKey, (key, md) -> {}, objectSize, 1000, cache);
        @Cleanup
        BlobStoreBackedInputStreamImpl second = new BlobStoreBackedInputStreamImpl(countingBlobStore, BUCKET,
                objectKey, (key, md) -> {}, objectSize, 1000, cache);
        // two readers 2,500 bytes apart, reading 100 bytes in turn, as cursors in the same ledger do
        second.seek(2500);
        byte[] buffer = new byte[100];
        for (int i = 0; i < 75; i++) {
            long firstPosition = first.getCurrentPosition();
            assertThat(first.readNBytes(buffer, 0, 100)).isEqualTo(100);
            assertThat(buffer).isEqualTo(Arrays.copyOfRange(data, (int) firstPosition,
                    (int) firstPosition + 100));
            long secondPosition = second.getCurrentPosition();
            assertThat(second.readNBytes(buffer, 0, 100)).isEqualTo(100);
            assertThat(buffer).isEqualTo(Arrays.copyOfRange(data, (int) secondPosition,
                    (int) secondPosition + 100));
        }
        // each of the 10 chunks of 1,000 bytes once, although the readers alternate
        verify(countingBlobStore, times(10)).getBlob(eq(BUCKET), eq(objectKey), any());
    }

    @Test
    public void skippingDoesNotFetchTheSkippedChunks() throws Exception {
        String objectKey = "skippedChunks";
        int objectSize = 10_000;
        byte[] data = new byte[objectSize];
        new Random(2).nextBytes(data);
        blobStore.putBlob(BUCKET, blobStore.blobBuilder(objectKey).payload(data).contentLength(objectSize).build());
        BlobStore countingBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
        @Cleanup
        BlobStoreBackedInputStreamImpl stream = new BlobStoreBackedInputStreamImpl(countingBlobStore, BUCKET,
                objectKey, (key, md) -> {}, objectSize, 1000);
        assertThat(stream.skip(5_500)).isEqualTo(5_500);
        assertThat(stream.read()).isEqualTo(data[5_500] & 0xFF);
        assertThat(stream.skip(100_000)).isEqualTo(objectSize - 5_501);
        assertThat(stream.read()).isEqualTo(-1);
        verify(countingBlobStore, times(1)).getBlob(eq(BUCKET), eq(objectKey), any());
    }

    @Test
    public void prefetchesTheNextChunkOfASequentialRead() throws Exception {
        String objectKey = "prefetch";
        int objectSize = 5_000;
        byte[] data = new byte[objectSize];
        new Random(3).nextBytes(data);
        blobStore.putBlob(BUCKET, blobStore.blobBuilder(objectKey).payload(data).contentLength(objectSize).build());
        @Cleanup
        OffloadedDataChunkCache cache = new OffloadedDataChunkCache(1 << 20, 60, 1, 2);
        @Cleanup
        BlobStoreBackedInputStreamImpl stream = new BlobStoreBackedInputStreamImpl(blobStore, BUCKET,
                objectKey, (key, md) -> {}, objectSize, 1000, cache);
        stream.readNBytes(1000);
        assertThat(cache.fetches()).isEqualTo(1);
        // moving on to the second chunk fetches the third in the background
        stream.read();
        Awaitility.await().untilAsserted(() -> assertThat(cache.fetches()).isEqualTo(3));
        Awaitility.await().untilAsserted(() -> assertThat(cache.cachedBytes()).isEqualTo(3000));
        stream.skip(999);
        stream.read();
        // the third chunk was prefetched
        assertThat(cache.hits()).isEqualTo(1);
    }
}
