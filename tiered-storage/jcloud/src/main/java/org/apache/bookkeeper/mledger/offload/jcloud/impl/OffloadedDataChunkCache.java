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
 * fetching it again. A reader that moves on to the next chunk has the chunks after it fetched in the background.
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

        Chunk(ByteBuf buffer) {
            this.buffer = buffer;
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
    }

    private final long maxSizeBytes;
    private final int prefetchChunks;
    private final Cache<Key, Chunk> chunks;
    private final ConcurrentHashMap<Key, PendingFetch> pendingFetches = new ConcurrentHashMap<>();
    private final ExecutorService fetchExecutor;
    private final ScheduledExecutorService cleanupExecutor;
    private volatile boolean closed;
    private final LongAdder hits = new LongAdder();
    private final LongAdder joinedFetches = new LongAdder();
    private final LongAdder fetches = new LongAdder();

    public OffloadedDataChunkCache(OffloadedReadCacheConfig config) {
        this(config.cacheSizeBytes(), config.cacheExpirySeconds(), config.prefetchChunks(), config.fetchThreads());
    }

    @VisibleForTesting
    OffloadedDataChunkCache(long maxSizeBytes, int ttlSeconds, int prefetchChunks, int fetchThreads) {
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
        this.fetchExecutor = Executors.newFixedThreadPool(Math.max(1, fetchThreads),
                new ThreadFactoryBuilder().setNameFormat("offloader-chunk-fetch-%d").setDaemon(true).build());
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("offloader-chunk-cache-cleanup").setDaemon(true).build());
        int period = Math.max(1, ttlSeconds / 2);
        // Expired chunks are otherwise only removed, and their buffers released, by later cache operations
        cleanupExecutor.scheduleAtFixedRate(chunks::cleanUp, period, period, TimeUnit.SECONDS);
    }

    /**
     * Returns the chunk with a reference that the caller releases: from the cache, from a fetch of it in progress, or
     * from a fetch that this call starts, waiting for the fetch.
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
                pending = startFetch(key, fetcher);
            } else {
                joinedFetches.increment();
            }
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

    /** Fetches the chunk in the background unless it is cached or being fetched. */
    void prefetch(Key key, Fetcher fetcher) {
        if (prefetchChunks == 0 || chunks.getIfPresent(key) != null || pendingFetches.containsKey(key)) {
            return;
        }
        startFetch(key, fetcher);
    }

    int prefetchChunks() {
        return prefetchChunks;
    }

    private PendingFetch startFetch(Key key, Fetcher fetcher) {
        PendingFetch created = new PendingFetch();
        PendingFetch existing = pendingFetches.putIfAbsent(key, created);
        if (existing != null) {
            return existing;
        }
        fetches.increment();
        try {
            fetchExecutor.execute(() -> fetch(key, fetcher, created));
        } catch (RuntimeException e) {
            // Such as a rejection after close
            complete(key, created, null, e);
        }
        return created;
    }

    private void fetch(Key key, Fetcher fetcher, PendingFetch pending) {
        ByteBuf buffer;
        try {
            buffer = fetcher.fetch(key);
        } catch (Throwable e) {
            complete(key, pending, null, e);
            return;
        }
        complete(key, pending, new Chunk(buffer), null);
    }

    private void complete(Key key, PendingFetch pending, Chunk chunk, Throwable failure) {
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
        if (chunk != null && maxSizeBytes > 0 && !closed) {
            chunk.retain(1);
            // Into the cache before it stops being pending, see acquire()
            chunks.put(key, chunk);
        }
        pendingFetches.remove(key, pending);
        if (chunk != null) {
            // The fetch's own reference, before the reads get the chunk: the cache and each of them hold theirs
            chunk.release();
            pending.future.complete(chunk);
        } else {
            log.warn().attr("bucket", key.bucket()).attr("key", key.key()).attr("start", key.start())
                    .attr("length", key.length()).exception(failure).log("Failed to fetch offloaded data");
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

    @VisibleForTesting
    long cachedBytes() {
        return chunks.asMap().keySet().stream().mapToLong(Key::length).sum();
    }

    public void clear() {
        chunks.invalidateAll();
    }

    @Override
    public void close() {
        closed = true;
        cleanupExecutor.shutdownNow();
        fetchExecutor.shutdownNow();
        // The reads waiting for fetches that won't run fail instead of waiting forever
        pendingFetches.forEach((key, pending) -> complete(key, pending, null,
                new IOException("The offloaded data chunk cache is closed")));
        chunks.invalidateAll();
    }
}
