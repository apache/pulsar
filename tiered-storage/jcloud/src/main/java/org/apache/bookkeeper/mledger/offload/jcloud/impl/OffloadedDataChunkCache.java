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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.OffloadedReadCacheConfig;

/**
 * The ranges of offloaded data objects that the read handles have fetched from the blob store, shared by every read
 * handle of the offloader factory: chunks of an object, aligned to the read buffer size, kept within a size budget and
 * dropped when no read has used them for a while. Cursors that read a ledger at different positions each keep their
 * chunk instead of replacing each other's, and a read of a chunk that is being fetched waits for that fetch instead of
 * fetching it again. A read fetches the chunk that it needs itself, on its thread. A reader that moves on to the next
 * chunk has the chunks after it fetched in the background, on the prefetch threads, at most as many at a time as there
 * are threads, so that a prefetch never waits in a queue.
 *
 * <p>Chunks are reference counted: the cache, the fetch that is completing it, and each reader using it hold a
 * reference, and the chunk's buffer is released when the last one is released.
 */
@CustomLog
public class OffloadedDataChunkCache implements AutoCloseable {
    /** A chunk of a data object: the bytes from {@code start}, {@code length} of them. */
    record Key(String bucket, String key, long start, int length) {
    }

    /** Fetches a chunk's bytes from the blob store into a buffer that the caller then owns. */
    @FunctionalInterface
    interface Fetcher {
        ByteBuf fetch(Key key) throws IOException;
    }

    /** A fetched chunk; its buffer is released when the last reference is released. */
    static final class Chunk {
        private final ByteBuf buffer;
        private final AtomicInteger references = new AtomicInteger(1);
        // Runs when the buffer is released; null for none
        private final Runnable onRelease;

        Chunk(ByteBuf buffer) {
            this(buffer, null);
        }

        Chunk(ByteBuf buffer, Runnable onRelease) {
            this.buffer = buffer;
            this.onRelease = onRelease;
        }

        ByteBuf buffer() {
            return buffer;
        }

        /** Takes a reference unless the chunk has already been released. */
        boolean tryRetain() {
            for (;;) {
                int current = references.get();
                if (current == 0) {
                    return false;
                }
                if (references.compareAndSet(current, current + 1)) {
                    return true;
                }
            }
        }

        void retain(int count) {
            if (count > 0) {
                references.addAndGet(count);
            }
        }

        void release() {
            if (references.decrementAndGet() == 0) {
                buffer.release();
                if (onRelease != null) {
                    onRelease.run();
                }
            }
        }

        @VisibleForTesting
        int references() {
            return references.get();
        }
    }

    /**
     * A fetch in progress, which later reads of the chunk join. Before it completes, the fetch takes a reference for
     * each read that joined it, so that the chunk can't be released between the fetch's completion and the reads.
     */
    private static final class PendingFetch {
        private final CompletableFuture<Chunk> future = new CompletableFuture<>();
        // Guarded by this
        private int waiters;
        private boolean done;
        private Chunk chunk;

        /**
         * @param waiters 1 for a fetch that a read starts, which is the fetch's first waiter from the start, so that
         *                it can't miss the chunk; 0 for a prefetch
         */
        PendingFetch(int waiters) {
            this.waiters = waiters;
        }
    }

    private final long maxSizeBytes;
    private final int prefetchChunks;
    private final Cache<Key, Chunk> chunks;
    private final ConcurrentHashMap<Key, PendingFetch> pendingFetches = new ConcurrentHashMap<>();
    private final ExecutorService prefetchExecutor;
    private final int prefetchThreads;
    // The prefetches queued or running, at most prefetchThreads
    private final AtomicInteger prefetchesInProgress = new AtomicInteger();
    private final ScheduledExecutorService cleanupExecutor;
    // Guarded by this
    private boolean closed;
    private static final String CLOSED_MESSAGE = "The offloaded data chunk cache is closed";
    private final LongAdder hits = new LongAdder();
    private final LongAdder joinedFetches = new LongAdder();
    private final LongAdder fetches = new LongAdder();
    // The fetched chunks whose buffers haven't been released yet
    private final LongAdder liveChunks = new LongAdder();

    public OffloadedDataChunkCache(OffloadedReadCacheConfig config) {
        this(config.cacheSizeBytes(), config.cacheExpirySeconds(), config.prefetchChunks(), config.prefetchThreads());
    }

    @VisibleForTesting
    OffloadedDataChunkCache(long maxSizeBytes, int ttlSeconds, int prefetchChunks, int prefetchThreads) {
        this.maxSizeBytes = Math.max(0, maxSizeBytes);
        // Prefetched chunks are only kept by the cache
        this.prefetchChunks = this.maxSizeBytes > 0 ? Math.max(0, prefetchChunks) : 0;
        this.chunks = CacheBuilder.newBuilder()
                // One segment, so that the budget and the least recently used order are the cache's, not each
                // segment's; the cache is used once per chunk read, which keeps contention low
                .concurrencyLevel(1)
                .maximumWeight(this.maxSizeBytes)
                .<Key, Chunk>weigher((key, chunk) -> key.length())
                .expireAfterAccess(Math.max(1, ttlSeconds), TimeUnit.SECONDS)
                .removalListener((RemovalListener<Key, Chunk>) notification -> notification.getValue().release())
                .build();
        this.prefetchThreads = Math.max(1, prefetchThreads);
        this.prefetchExecutor = Executors.newFixedThreadPool(this.prefetchThreads,
                new ThreadFactoryBuilder().setNameFormat("offloader-chunk-prefetch-%d").setDaemon(true).build());
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("offloader-chunk-cache-cleanup").setDaemon(true).build());
        int period = Math.max(1, ttlSeconds / 2);
        // Expired chunks are otherwise only removed, and their buffers released, by later cache operations
        cleanupExecutor.scheduleAtFixedRate(chunks::cleanUp, period, period, TimeUnit.SECONDS);
    }

    /**
     * Returns the chunk with a reference that the caller releases: from the cache, from a fetch of it in progress, or
     * from a fetch that this call makes on the calling thread.
     */
    Chunk acquire(Key key, Fetcher fetcher) throws IOException {
        for (;;) {
            Chunk cached = chunks.getIfPresent(key);
            if (cached != null && cached.tryRetain()) {
                hits.increment();
                return cached;
            }
            PendingFetch pending = pendingFetches.get(key);
            if (pending == null) {
                // A fetch that completed in between has put its chunk into the cache before it stopped being pending
                cached = chunks.getIfPresent(key);
                if (cached != null && cached.tryRetain()) {
                    hits.increment();
                    return cached;
                }
                PendingFetch created = new PendingFetch(1);
                pending = pendingFetches.putIfAbsent(key, created);
                if (pending == null) {
                    // This read is the fetch's first waiter already
                    fetchOrUseCached(key, fetcher, created);
                    return await(created);
                }
            }
            joinedFetches.increment();
            synchronized (pending) {
                if (pending.done) {
                    if (pending.chunk != null && pending.chunk.tryRetain()) {
                        return pending.chunk;
                    }
                    if (pending.chunk != null) {
                        // Evicted since its fetch completed: fetch it again
                        continue;
                    }
                } else {
                    pending.waiters++;
                }
            }
            return await(pending);
        }
    }

    /**
     * Fetches the chunk in the background unless it is cached or being fetched, or as many prefetches as there are
     * prefetch threads are in progress.
     */
    void prefetch(Key key, Fetcher fetcher) {
        // A chunk larger than the budget would be dropped at once, before a read uses it
        if (prefetchChunks == 0 || key.length() > maxSizeBytes || chunks.getIfPresent(key) != null
                || pendingFetches.containsKey(key)) {
            return;
        }
        // Queued and running together, so that a prefetch, which a read may join, never waits in a queue
        if (prefetchesInProgress.incrementAndGet() > prefetchThreads) {
            prefetchesInProgress.decrementAndGet();
            return;
        }
        PendingFetch created = new PendingFetch(0);
        if (pendingFetches.putIfAbsent(key, created) != null) {
            prefetchesInProgress.decrementAndGet();
            return;
        }
        try {
            prefetchExecutor.execute(() -> {
                try {
                    fetchOrUseCached(key, fetcher, created);
                } finally {
                    prefetchesInProgress.decrementAndGet();
                }
            });
        } catch (RejectedExecutionException e) {
            prefetchesInProgress.decrementAndGet();
            // After close: a read that joined it fails as the reads of a closed cache do
            complete(key, created, null, new IOException(CLOSED_MESSAGE, e), false);
        }
    }

    int prefetchChunks() {
        return prefetchChunks;
    }

    /** Completes the pending fetch with the cached chunk, or with a fetch of it on the calling thread. */
    private void fetchOrUseCached(Key key, Fetcher fetcher, PendingFetch pending) {
        // A fetch that completed since the caller's last look at the cache has cached the chunk
        Chunk cached = chunks.getIfPresent(key);
        if (cached != null && cached.tryRetain()) {
            // The retained reference is the fetch's own
            complete(key, pending, cached, null, false);
            return;
        }
        fetches.increment();
        fetch(key, fetcher, pending);
    }

    private void fetch(Key key, Fetcher fetcher, PendingFetch pending) {
        ByteBuf buffer;
        try {
            buffer = fetcher.fetch(key);
        } catch (Throwable e) {
            complete(key, pending, null, e, false);
            return;
        }
        liveChunks.increment();
        complete(key, pending, new Chunk(buffer, liveChunks::decrement), null, true);
    }

    /**
     * Completes the fetch with the chunk, with a reference that it releases, or with the failure. A fetched chunk is
     * {@code publish}ed into the cache.
     */
    private void complete(Key key, PendingFetch pending, Chunk chunk, Throwable failure, boolean publish) {
        synchronized (pending) {
            if (pending.done) {
                // close() has failed it already
                if (chunk != null) {
                    chunk.release();
                }
                return;
            }
            pending.done = true;
            pending.chunk = chunk;
            if (chunk != null) {
                // A reference for each read that joined the fetch; later ones retain the chunk themselves
                chunk.retain(pending.waiters);
            }
        }
        if (chunk != null && publish && maxSizeBytes > 0) {
            // With close(), so that no chunk is put into the cache after close() has emptied it
            synchronized (this) {
                if (!closed) {
                    chunk.retain(1);
                    // Into the cache before it stops being pending, see acquire()
                    chunks.put(key, chunk);
                }
            }
        }
        pendingFetches.remove(key, pending);
        if (chunk != null) {
            // The fetch's own reference, before the reads get the chunk: the cache and each of them hold theirs
            chunk.release();
            pending.future.complete(chunk);
        } else {
            if (isClosed()) {
                // Failed by the close, or interrupted by it
                log.debug().attr("bucket", key.bucket()).attr("key", key.key()).attr("start", key.start())
                        .attr("length", key.length()).exception(failure).log("Fetch of offloaded data ended by close");
            } else {
                log.warn().attr("bucket", key.bucket()).attr("key", key.key()).attr("start", key.start())
                        .attr("length", key.length()).exception(failure).log("Failed to fetch offloaded data");
            }
            pending.future.completeExceptionally(failure);
        }
    }

    private static Chunk await(PendingFetch pending) throws IOException {
        try {
            return pending.future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // The reference that the fetch took for this read is released when the fetch completes
            pending.future.thenAccept(Chunk::release);
            throw new InterruptedIOException("Interrupted while waiting for offloaded data");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Error reading from BlobStore", cause);
        }
    }

    private synchronized boolean isClosed() {
        return closed;
    }

    @VisibleForTesting
    long hits() {
        return hits.sum();
    }

    @VisibleForTesting
    long joinedFetches() {
        return joinedFetches.sum();
    }

    @VisibleForTesting
    long fetches() {
        return fetches.sum();
    }

    /** The reads waiting for the fetch of the chunk, or -1 when it isn't being fetched. */
    @VisibleForTesting
    int waiters(Key key) {
        PendingFetch pending = pendingFetches.get(key);
        if (pending == null) {
            return -1;
        }
        synchronized (pending) {
            return pending.waiters;
        }
    }

    /** The fetched chunks whose buffers haven't been released yet: cached, or used by a read. */
    @VisibleForTesting
    long liveChunks() {
        return liveChunks.sum();
    }

    @VisibleForTesting
    long cachedBytes() {
        return chunks.asMap().keySet().stream().mapToLong(Key::length).sum();
    }

    public void clear() {
        chunks.invalidateAll();
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
        }
        cleanupExecutor.shutdownNow();
        prefetchExecutor.shutdownNow();
        // The reads waiting for prefetches that won't run fail instead of waiting forever
        pendingFetches.forEach((key, pending) -> complete(key, pending, null, new IOException(CLOSED_MESSAGE), false));
        synchronized (this) {
            chunks.invalidateAll();
        }
    }
}
