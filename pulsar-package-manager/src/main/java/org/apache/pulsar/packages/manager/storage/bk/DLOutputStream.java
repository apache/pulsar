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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * DistributedLog Output Stream.
 */
@Slf4j
class DLOutputStream {

    private final DistributedLogManager distributedLogManager;
    private final AsyncLogWriter writer;
    private long offset = 0L;

    private DLOutputStream(DistributedLogManager distributedLogManager, AsyncLogWriter writer) {
        this.distributedLogManager = distributedLogManager;
        this.writer = writer;
    }

    static CompletableFuture<DLOutputStream> openWriterAsync(DistributedLogManager distributedLogManager) {
        log.info("Open a dlog manager");
        return distributedLogManager.openAsyncLogWriter().thenApply(w -> new DLOutputStream(distributedLogManager, w));
    }

    /**
     * Write all input stream data to the distribute log.
     *
     * @param inputStream the data we need to write
     * @return
     */
    CompletableFuture<DLOutputStream> writeAsync(InputStream inputStream) {
        CompletableFuture<DLOutputStream> result = new CompletableFuture<>();
        List<CompletableFuture<DLOutputStream>> writeFuture = new ArrayList<>();
        CompletableFuture.runAsync(() -> {
            byte[] readBuffer = new byte[1024];
            try {
                int read = 0;
                while ((read = inputStream.read(readBuffer)) != -1) {
                    log.info("write something into the ledgers");
                    ByteBuf writeBuf = Unpooled.wrappedBuffer(readBuffer, 0, read);
                    writeFuture.add(writeAsync(writeBuf));
                }
                log.info("write done, and add to the future");
            } catch (IOException e) {
                e.printStackTrace();
                result.completeExceptionally(e);
            }
        }).whenComplete((ignore, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
                return;
            }
            log.info("All pending request has {}", writeFuture.size());
            FutureUtil.waitForAll(writeFuture)
                .whenComplete((i, error) -> {
                    log.info("Done the request");
                    if (error != null) {
                        error.printStackTrace();
                        result.completeExceptionally(error);
                    } else {
                        result.complete(this);
                    }
                });
        });


        return result;
    }

    /**
     * Write a ByteBuf data to the distribute log.
     *
     * @param data the data we need to write
     * @return
     */
    private CompletableFuture<DLOutputStream> writeAsync(ByteBuf data) {
        offset += data.readableBytes();
        LogRecord record = new LogRecord(offset, data);
        log.info("execute write to the dlog");
        return writer.write(record).whenComplete((dlsn, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
            } else {
                log.info("DLSN is {}", dlsn.toString());
            }
        }).thenApply(ignore -> this);
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
