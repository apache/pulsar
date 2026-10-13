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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.DataBlockUtils.VersionCheck;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;

/**
 * Reads an offloaded data object in chunks of the read buffer size, aligned to it. With an
 * {@link OffloadedDataChunkCache}, the chunks are shared with the other read handles: a chunk that another read has
 * fetched, or is fetching, isn't fetched again, and moving on to the next chunk fetches the ones after it in the
 * background. Without one, each chunk is fetched for this stream alone, when it is read.
 */
@CustomLog
public class BlobStoreBackedInputStreamImpl extends BackedInputStream {

    private final BlobStore blobStore;
    private final String bucket;
    private final String key;
    private final VersionCheck versionCheck;
    private final long objectLen;
    private final int bufferSize;
    private final OffloadedDataChunkCache chunkCache;
    private LedgerOffloaderStats offloaderStats;
    private String managedLedgerName;
    private String topicName;

    // The position of the next byte to read
    private long position;
    // The chunk that holds the bytes from chunkStart, with a reference that this stream releases; null when none
    private OffloadedDataChunkCache.Chunk chunk;
    private long chunkStart = -1;
    private int chunkLength;

    public BlobStoreBackedInputStreamImpl(BlobStore blobStore, String bucket, String key,
                                          VersionCheck versionCheck,
                                          long objectLen, int bufferSize) {
        this(blobStore, bucket, key, versionCheck, objectLen, bufferSize, (OffloadedDataChunkCache) null);
    }

    public BlobStoreBackedInputStreamImpl(BlobStore blobStore, String bucket, String key,
                                          VersionCheck versionCheck,
                                          long objectLen, int bufferSize, OffloadedDataChunkCache chunkCache) {
        this.blobStore = blobStore;
        this.bucket = bucket;
        this.key = key;
        this.versionCheck = versionCheck;
        this.objectLen = objectLen;
        this.bufferSize = bufferSize;
        this.chunkCache = chunkCache;
        this.position = 0;
    }

    public BlobStoreBackedInputStreamImpl(BlobStore blobStore, String bucket, String key,
                                          VersionCheck versionCheck,
                                          long objectLen, int bufferSize,
                                          LedgerOffloaderStats offloaderStats, String managedLedgerName) {
        this(blobStore, bucket, key, versionCheck, objectLen, bufferSize, offloaderStats, managedLedgerName, null);
    }

    public BlobStoreBackedInputStreamImpl(BlobStore blobStore, String bucket, String key,
                                          VersionCheck versionCheck,
                                          long objectLen, int bufferSize,
                                          LedgerOffloaderStats offloaderStats, String managedLedgerName,
                                          OffloadedDataChunkCache chunkCache) {
        this(blobStore, bucket, key, versionCheck, objectLen, bufferSize, chunkCache);
        this.offloaderStats = offloaderStats;
        this.managedLedgerName = managedLedgerName;
        this.topicName = TopicName.fromPersistenceNamingEncoding(managedLedgerName);
    }

    /**
     * Makes the current chunk the one that holds the byte at the position.
     * @return true if there are bytes to read, false otherwise
     */
    private boolean ensureChunk() throws IOException {
        if (position >= objectLen) {
            return false;
        }
        if (chunk != null && position >= chunkStart && position < chunkStart + chunkLength) {
            return true;
        }
        long start = position - position % bufferSize;
        int length = (int) Math.min(bufferSize, objectLen - start);
        boolean sequential = chunk != null && start == chunkStart + chunkLength;
        releaseChunk();
        OffloadedDataChunkCache.Key chunkKey = new OffloadedDataChunkCache.Key(bucket, key, start, length);
        if (chunkCache != null) {
            chunk = chunkCache.acquire(chunkKey, this::fetch);
            if (sequential) {
                prefetchAfter(start + length);
            }
        } else {
            chunk = new OffloadedDataChunkCache.Chunk(fetch(chunkKey));
        }
        chunkStart = start;
        chunkLength = length;
        return true;
    }

    private void prefetchAfter(long start) {
        for (int i = 0; i < chunkCache.prefetchChunks() && start < objectLen; i++) {
            int length = (int) Math.min(bufferSize, objectLen - start);
            chunkCache.prefetch(new OffloadedDataChunkCache.Key(bucket, key, start, length), this::fetch);
            start += length;
        }
    }

    /** Fetches the chunk's bytes from the blob store. */
    private ByteBuf fetch(OffloadedDataChunkCache.Key chunkKey) throws IOException {
        long startRange = chunkKey.start();
        long endRange = startRange + chunkKey.length() - 1;
        log.debug().attr("startRange", startRange).attr("endRange", endRange)
                .attr("bytesToFill", endRange - startRange)
                .log("Fetching chunk");
        ByteBuf buffer = PulsarByteBufAllocator.DEFAULT.buffer(chunkKey.length(), chunkKey.length());
        try {
            long startReadTime = System.nanoTime();
            Blob blob = blobStore.getBlob(bucket, key, new GetOptions().range(startRange, endRange));
            if (blob == null) {
                throw new KeyNotFoundException(bucket, key, "");
            }
            versionCheck.check(key, blob);

            try (InputStream stream = blob.getPayload().openStream()) {
                fillBuffer(buffer, stream, chunkKey.length());
            }
            if (buffer.readableBytes() != chunkKey.length()) {
                throw new IOException("Read " + buffer.readableBytes() + " of the " + chunkKey.length()
                        + " bytes from " + startRange + " of " + key);
            }

            // here we can get the metrics
            // because JClouds streams the content
            // and actually the HTTP call finishes when the stream is fully read
            if (this.offloaderStats != null) {
                this.offloaderStats.recordReadOffloadDataLatency(topicName,
                        System.nanoTime() - startReadTime, TimeUnit.NANOSECONDS);
                this.offloaderStats.recordReadOffloadBytes(topicName, endRange - startRange + 1);
            }
            return buffer;
        } catch (Throwable e) {
            buffer.release();
            if (null != this.offloaderStats) {
                this.offloaderStats.recordReadOffloadError(this.topicName);
            }
            // If the blob is not found, the original exception is thrown and handled by the caller.
            if (e instanceof KeyNotFoundException) {
                throw e;
            }
            throw new IOException("Error reading from BlobStore", e);
        }
    }

    static void fillBuffer(ByteBuf buffer, InputStream is, int bytesToCopy) throws IOException {
        while (bytesToCopy > 0) {
            int writeBytes = buffer.writeBytes(is, bytesToCopy);
            if (writeBytes < 0) {
                break;
            }
            bytesToCopy -= writeBytes;
        }
    }

    private void releaseChunk() {
        if (chunk != null) {
            chunk.release();
            chunk = null;
            chunkStart = -1;
            chunkLength = 0;
        }
    }

    @Override
    public int read() throws IOException {
        if (ensureChunk()) {
            // By absolute index: other streams may read the same chunk
            int value = chunk.buffer().getUnsignedByte((int) (position - chunkStart));
            position++;
            return value;
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (ensureChunk()) {
            int index = (int) (position - chunkStart);
            int bytesToRead = Math.min(len, chunkLength - index);
            chunk.buffer().getBytes(index, b, off, bytesToRead);
            position += bytesToRead;
            return bytesToRead;
        } else {
            return -1;
        }
    }

    /** Skips without reading the skipped bytes, so that a chunk that is skipped entirely isn't fetched. */
    @Override
    public long skip(long n) {
        if (n <= 0) {
            return 0;
        }
        long skipped = Math.min(n, Math.max(0, objectLen - position));
        position += skipped;
        return skipped;
    }

    @Override
    public void seek(long position) {
        log.debug().attr("position", position).attr("bucket", bucket).attr("key", key)
                .attr("currentPosition", this.position).attr("chunkStart", chunkStart)
                .attr("chunkLength", chunkLength).log("Seeking");
        // The current chunk is kept: a seek within it doesn't fetch
        this.position = position;
    }

    @Override
    public void seekForward(long position) throws IOException {
        if (position >= this.position) {
            seek(position);
        } else {
            throw new IOException(String.format("Error seeking, new position %d < current position %d",
                                                position, this.position));
        }
    }

    public long getCurrentPosition() {
        return position;
    }

    @Override
    public void close() {
        releaseChunk();
    }

    @Override
    public int available() throws IOException {
        long available = objectLen - position;
        return available > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(0, available);
    }
}
