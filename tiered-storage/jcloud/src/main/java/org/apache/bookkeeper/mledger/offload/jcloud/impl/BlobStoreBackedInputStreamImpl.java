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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.DataBlockUtils.VersionCheck;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStoreBackedInputStreamImpl extends BackedInputStream {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreBackedInputStreamImpl.class);

    private final BlobStore blobStore;
    private final String bucket;
    private final String key;
    private final VersionCheck versionCheck;
    private final ByteBuf buffer;
    private final long objectLen;
    private final int bufferSize;

    private long cursor;
    private long bufferOffsetStart;
    private long bufferOffsetEnd;

    public BlobStoreBackedInputStreamImpl(BlobStore blobStore, String bucket, String key,
                                          VersionCheck versionCheck,
                                          long objectLen, int bufferSize) {
        this.blobStore = blobStore;
        this.bucket = bucket;
        this.key = key;
        this.versionCheck = versionCheck;
        this.buffer = PulsarByteBufAllocator.DEFAULT.buffer(bufferSize, bufferSize);
        this.objectLen = objectLen;
        this.bufferSize = bufferSize;
        this.cursor = 0;
        this.bufferOffsetStart = this.bufferOffsetEnd = -1;
    }

    /**
     * Refill the buffered input if it is empty.
     * @return true if there are bytes to read, false otherwise
     */
    private boolean refillBufferIfNeeded() throws IOException {
        if (buffer.readableBytes() == 0) {
            if (cursor >= objectLen) {
                return false;
            }
            long startRange = cursor;
            long endRange = Math.min(cursor + bufferSize - 1,
                                     objectLen - 1);

            try {
                Blob blob = blobStore.getBlob(bucket, key, new GetOptions().range(startRange, endRange));
                versionCheck.check(key, blob);

                try (InputStream stream = blob.getPayload().openStream()) {
                    buffer.clear();
                    bufferOffsetStart = startRange;
                    bufferOffsetEnd = endRange;
                    long bytesRead = endRange - startRange + 1;
                    int bytesToCopy = (int) bytesRead;
                    while (bytesToCopy > 0) {
                        bytesToCopy -= buffer.writeBytes(stream, bytesToCopy);
                    }
                    cursor += buffer.readableBytes();
                }
            } catch (Throwable e) {
                throw new IOException("Error reading from BlobStore", e);
            }
        }
        return true;
    }

    @Override
    public int read() throws IOException {
        if (refillBufferIfNeeded()) {
            return buffer.readUnsignedByte();
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (refillBufferIfNeeded()) {
            int bytesToRead = Math.min(len, buffer.readableBytes());
            buffer.readBytes(b, off, bytesToRead);
            return bytesToRead;
        } else {
            return -1;
        }
    }

    @Override
    public void seek(long position) {
        log.debug("Seeking to {} on {}/{}, current position {} (bufStart:{}, bufEnd:{})",
                position, bucket, key, cursor, bufferOffsetStart, bufferOffsetEnd);
        if (position >= bufferOffsetStart && position <= bufferOffsetEnd) {
            long newIndex = position - bufferOffsetStart;
            buffer.readerIndex((int) newIndex);
        } else {
            this.cursor = position;
            buffer.clear();
        }
    }

    @Override
    public void seekForward(long position) throws IOException {
        if (position >= cursor) {
            seek(position);
        } else {
            throw new IOException(String.format("Error seeking, new position %d < current position %d",
                                                position, cursor));
        }
    }

    @Override
    public void close() {
        buffer.release();
    }
}
