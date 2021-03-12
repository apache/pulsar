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
package org.apache.pulsar.packages.management.storage.bookkeeper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
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
     * @param outputStream the data write to
     * @return
     */
    CompletableFuture<DLInputStream> readAsync(OutputStream outputStream) {
        CompletableFuture<Void> outputFuture = new CompletableFuture<>();
        read(outputStream, outputFuture, 10);
        return outputFuture.thenApply(ignore -> this);
    }

    /**
     * When reading the end of a stream, it will throw an EndOfStream exception. So we can use this to
     * check if we read to the end.
     *
     * @param outputStream the data write to
     * @param readFuture a future that wait to read complete
     * @param num how many entries read in one time
     */
    private void read(OutputStream outputStream, CompletableFuture<Void> readFuture, int num) {
        reader.readBulk(num)
            .whenComplete((logRecordWithDLSNS, throwable) -> {
                if (null != throwable) {
                    if (throwable instanceof EndOfStreamException) {
                        readFuture.complete(null);
                    } else {
                        readFuture.completeExceptionally(throwable);
                    }
                    return;
                }
                CompletableFuture.runAsync(() -> {
                    logRecordWithDLSNS.forEach(logRecord -> {
                        try {
                            outputStream.write(logRecord.getPayload());
                        } catch (IOException e) {
                            readFuture.completeExceptionally(e);
                        }
                    });
                }).thenRun(() -> read(outputStream, readFuture, num));
            });
    }

    CompletableFuture<Void> closeAsync() {
        return reader.asyncClose().thenCompose(ignore -> distributedLogManager.asyncClose());
    }
}

