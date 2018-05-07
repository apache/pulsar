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
package org.apache.pulsar.broker.s3offload.impl;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.InputStream;
import java.io.IOException;

import org.apache.pulsar.broker.s3offload.S3BackedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3BackedInputStreamImpl extends S3BackedInputStream {
    private static final Logger log = LoggerFactory.getLogger(S3BackedInputStreamImpl.class);

    private final AmazonS3 s3client;
    private final String bucket;
    private final String key;
    private final ByteBuf buffer;
    private final long objectLen;
    private final int bufferSize;

    private long cursor;

    public S3BackedInputStreamImpl(AmazonS3 s3client, String bucket, String key,
                                   long objectLen, int bufferSize) {
        this.s3client = s3client;
        this.bucket = bucket;
        this.key = key;
        this.buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(bufferSize, bufferSize);
        this.objectLen = objectLen;
        this.bufferSize = bufferSize;
        this.cursor = 0;
    }

    @Override
    public int read() throws IOException {
        if (buffer.readableBytes() == 0) {
            if (cursor >= objectLen) {
                return -1;
            }
            long startRange = cursor;
            long endRange = Math.min(cursor + bufferSize - 1,
                                     objectLen - 1);
            GetObjectRequest req = new GetObjectRequest(bucket, key)
                .withRange(startRange, endRange);
            log.debug("Reading range {}-{} from {}/{}", startRange, endRange, bucket, key);
            try (S3Object obj = s3client.getObject(req)) {
                Long[] range = obj.getObjectMetadata().getContentRange();
                long bytesRead = range[1] - range[0] + 1;

                buffer.clear();
                InputStream s = obj.getObjectContent();
                long bytesToCopy = bytesRead;
                while (bytesToCopy > 0) {
                    bytesToCopy -= buffer.writeBytes(s, (int)bytesToCopy);
                }
                cursor += buffer.readableBytes();
            } catch (AmazonClientException e) {
                throw new IOException("Error reading from S3", e);
            }
        }
        return buffer.readUnsignedByte();
    }

    @Override
    public void seek(long position) {
        log.debug("Seeking to {} on {}/{}, current position {}", position, bucket, key, cursor);
        this.cursor = position;
        buffer.clear();
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
