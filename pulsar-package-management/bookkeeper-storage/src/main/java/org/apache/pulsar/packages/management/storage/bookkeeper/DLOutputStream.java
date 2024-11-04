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
package org.apache.pulsar.packages.management.storage.bookkeeper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;

/**
 * DistributedLog Output Stream.
 */
@Slf4j
class DLOutputStream {

    private final DistributedLogManager distributedLogManager;
    private final AsyncLogWriter writer;
    /*
     * The LogRecord structure is:
     * -------------------
     * Bytes 0 - 7                      : Metadata (Long)
     * Bytes 8 - 15                     : TxId (Long)
     * Bytes 16 - 19                    : Payload length (Integer)
     * Bytes 20 - 20+payload.length-1   : Payload (Byte[])
     * So the max buffer size should be LogRecord.MAX_LOGRECORD_SIZE - 2 * (Long.SIZE / 8) - Integer.SIZE / 8
     */
    private final byte[] readBuffer = new byte[LogRecord.MAX_LOGRECORD_SIZE - 2 * (Long.SIZE / 8) - Integer.SIZE / 8];
    private long offset = 0L;

    private DLOutputStream(DistributedLogManager distributedLogManager, AsyncLogWriter writer) {
        this.distributedLogManager = distributedLogManager;
        this.writer = writer;
    }

    static CompletableFuture<DLOutputStream> openWriterAsync(DistributedLogManager distributedLogManager) {
        log.info("Open a dlog manager");
        return distributedLogManager.openAsyncLogWriter().thenApply(w -> new DLOutputStream(distributedLogManager, w));
    }

    private void writeAsyncHelper(InputStream is, CompletableFuture<DLOutputStream> result) {
        try {
            int read = is.readNBytes(readBuffer, 0, readBuffer.length);
            if (read > 0) {
                log.debug("write something into the ledgers offset: {}, length: {}", offset, read);
                final ByteBuf writeBuf = Unpooled.wrappedBuffer(readBuffer, 0, read);
                offset += writeBuf.readableBytes();
                final LogRecord record = new LogRecord(offset, writeBuf);
                writer.write(record).thenAccept(v -> writeAsyncHelper(is, result))
                        .exceptionally(e -> {
                            result.completeExceptionally(e);
                            return null;
                        });
            } else {
                result.complete(this);
            }
        } catch (IOException e) {
            log.error("Failed to get all records from the input stream", e);
            result.completeExceptionally(e);
        }
    }

    /**
     * Write all input stream data to the distribute log.
     *
     * @param inputStream the data we need to write
     * @return CompletableFuture<DLOutputStream>
     */
    CompletableFuture<DLOutputStream> writeAsync(InputStream inputStream) {
        CompletableFuture<DLOutputStream> result = new CompletableFuture<>();
        writeAsyncHelper(inputStream, result);
        return result;
    }

    /**
     * Every package will be a stream. So we need mark the stream as EndOfStream when the stream
     * write done.
     *
     * @return
     */
    CompletableFuture<Void> closeAsync() {
        return writer.markEndOfStream()
            .thenCompose(ignore -> writer.asyncClose())
            .thenCompose(ignore -> distributedLogManager.asyncClose());
    }
}

