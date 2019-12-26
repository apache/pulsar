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

import io.netty.buffer.Unpooled;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Unit test of {@link DLOutputStream}.
 */
public class DLOutputStreamTest {

    private DistributedLogManager dlm;
    private AsyncLogWriter writer;

    @BeforeMethod
    public void setup() {
        dlm = mock(DistributedLogManager.class);
        writer = mock(AsyncLogWriter.class);

        when(dlm.openAsyncLogWriter()).thenReturn(CompletableFuture.completedFuture(writer));
        when(dlm.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        when(writer.markEndOfStream()).thenReturn(CompletableFuture.completedFuture(null));
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        when(writer.write(any(LogRecord.class))).thenReturn(CompletableFuture.completedFuture(DLSN.InitialDLSN));
    }

    @AfterMethod
    public void teardown() throws IOException {
        if (dlm != null) {
            dlm.close();
        }
    }

    /**
     * Test Case: write data using input stream.
     */
    @Test
    public void writeInputStreamData() throws ExecutionException, InterruptedException {
        byte[] data = "test-write".getBytes();
        DLOutputStream.openWriterAsync(dlm)
            .thenCompose(w -> w.writeAsync(new ByteArrayInputStream(data))
                .thenCompose(DLOutputStream::closeAsync)).get();

        verify(writer, times(1)).write(any(LogRecord.class));
        verify(writer, times(1)).markEndOfStream();
        verify(writer, times(1)).asyncClose();
        verify(dlm, times(1)).asyncClose();
    }

    /**
     * Test Case: write data with byte array.
     */
    @Test
    public void writeBytesArrayData() throws ExecutionException, InterruptedException {
        byte[] data = "test-write".getBytes();
        DLOutputStream.openWriterAsync(dlm)
            .thenCompose(w -> w.writeAsync(Unpooled.wrappedBuffer(data))
                .thenCompose(DLOutputStream::closeAsync)).get();

        verify(writer, times(1)).write(any(LogRecord.class));
        verify(writer, times(1)).markEndOfStream();
        verify(writer, times(1)).asyncClose();
        verify(dlm, times(1)).asyncClose();
    }

    @Test
    public void openAsyncLogWriterFailed() {
        when(dlm.openAsyncLogWriter()).thenReturn(FutureUtil.failedFuture(new Exception("Open writer was failed")));

        try {
            DLOutputStream.openWriterAsync(dlm).get();
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(), "Open writer was failed");
        }
    }

    @Test
    public void writeRecordFailed() {
        when(writer.write(any(LogRecord.class))).thenReturn(FutureUtil.failedFuture(new Exception("Write data was failed")));

        byte[] data = "test-write".getBytes();
        try {
            DLOutputStream.openWriterAsync(dlm)
                .thenCompose(w -> w.writeAsync(new ByteArrayInputStream(data)))
                .thenCompose(DLOutputStream::closeAsync).get();
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(), "Write data was failed");
        }
    }
}
