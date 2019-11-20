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

package org.apache.pulsar.packages.manager.storage.bk;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.exceptions.EndOfStreamException;

/**
 * DistributedLog Input Stream.
 */
class DLInputStream {

    private final DistributedLogManager distributedLogManager;
    private final AsyncLogReader reader;

    private DLInputStream(DistributedLogManager distributedLogManager, AsyncLogReader reader) {
        this.distributedLogManager = distributedLogManager;
        this.reader = reader;
    }

    static CompletableFuture<DLInputStream> openReaderAsync(DistributedLogManager distributedLogManager) {
        return distributedLogManager.openAsyncLogReader(DLSN.InitialDLSN)
            .thenApply(r -> new DLInputStream(distributedLogManager, r));
    }

    /**
     * Read data to output stream.
     *
     * @param outputStream
     * @return
     */
    CompletableFuture<DLInputStream> readAsync(OutputStream outputStream) {
        CompletableFuture<DLInputStream> future = new CompletableFuture<>();

        CompletableFuture<ByteBuf> outputFuture = new CompletableFuture<>();
        ByteBuf data = Unpooled.buffer();
        read(data,outputFuture, 10);
        outputFuture.whenComplete((byteBuf, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
                return;
            }

            if (byteBuf != null) {
                try {
                    byteBuf.readBytes(outputStream, byteBuf.readableBytes());
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
                byteBuf.release();
            }

            future.complete(this);
        });

        return future;
    }

    /**
     * Read data to a bytes array.
     *
     * @return
     */
    CompletableFuture<ByteResult> readAsync() {
        CompletableFuture<ByteResult> result = new CompletableFuture<>();

        CompletableFuture<ByteBuf> entriesFuture = new CompletableFuture<>();
        ByteBuf data = Unpooled.buffer();
        read(data, entriesFuture, 1);
        entriesFuture.whenComplete((byteBuf, throwable) -> {
            if (null != throwable) {
                result.completeExceptionally(throwable);
                return;
            }

            byteBuf.capacity(byteBuf.readableBytes());
            result.complete(new ByteResult(this, byteBuf.array()));
            byteBuf.release();
        });

        return result;
    }

    /**
     * When reading the end of a stream, it will throw an EndOfStream exception. So we can use this to
     * check if we read to the end.
     *
     * @param byteBuf received data
     * @param result
     * @param num how many entries read in one time
     */
    private void read(ByteBuf byteBuf,CompletableFuture<ByteBuf> result, int num) {
        reader.readBulk(num)
            .whenComplete((logRecordWithDLSNS, throwable) -> {
                if (null != throwable) {
                    if (throwable instanceof EndOfStreamException) {
                        result.complete(byteBuf);
                    } else {
                        result.completeExceptionally(throwable);
                    }
                    return;
                }
                logRecordWithDLSNS.forEach(logRecord -> byteBuf.writeBytes(logRecord.getPayload()));
                read(byteBuf, result, num);
            });
    }

    CompletableFuture<Void> closeAsync() {
        return reader.asyncClose().thenCompose(ignore -> distributedLogManager.asyncClose());
    }

    @Data
    @AllArgsConstructor
    static class ByteResult {
        private DLInputStream dlInputStream;
        private byte[] data;

        CompletableFuture<byte[]> getResult() {
            return dlInputStream.closeAsync().thenApply(ignore -> this.data);
        }
    }
}
