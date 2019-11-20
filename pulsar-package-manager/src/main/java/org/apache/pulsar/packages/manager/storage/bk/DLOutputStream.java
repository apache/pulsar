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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * DistributedLog Output Stream.
 */
class DLOutputStream {

    private final DistributedLogManager distributedLogManager;
    private final AsyncLogWriter writer;
    private long offset = 0L;

    private DLOutputStream(DistributedLogManager distributedLogManager, AsyncLogWriter writer) {
        this.distributedLogManager = distributedLogManager;
        this.writer = writer;
    }

    static CompletableFuture<DLOutputStream> openWriterAsync(DistributedLogManager distributedLogManager) {
        return distributedLogManager.openAsyncLogWriter().thenApply(w -> new DLOutputStream(distributedLogManager, w));
    }

    /**
     * Write all input stream data to the distribute log.
     *
     * @param inputStream the data we need to write
     * @return
     */
    CompletableFuture<DLOutputStream> writeAsync(InputStream inputStream) {
        List<CompletableFuture<DLOutputStream>> writeFuture = new ArrayList<>();
        byte[] bytes = new byte[1024];
        try {
            while (inputStream.read(bytes) != -1) {
                writeFuture.add(writeAsync(bytes));
            }
        } catch (IOException e) {
            return FutureUtil.failedFuture(e);
        }
        return FutureUtil.waitForAll(writeFuture).thenApply(ignore -> this);
    }

    /**
     * Write a bytes array data to the distribute log.
     *
     * @param data the data we need to write
     * @return
     */
    CompletableFuture<DLOutputStream> writeAsync(byte[] data) {
        offset += data.length;
        LogRecord record = new LogRecord(offset, data);
        return writer.write(record).thenApply(ignore -> this);
    }

    /**
     * Every package will be a stream. So we need mark the stream as EndOfStream when the stream
     * write done.
     *
     * @return
     */
    CompletableFuture<Void> closeAsync() {
        return writer
            .markEndOfStream()
            .thenCompose(ignore -> writer.asyncClose())
            .thenCompose(ignore -> distributedLogManager.asyncClose());
    }
}
