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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.eq;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DLInputStreamTest {
    private DistributedLogManager dlm;
    private AsyncLogReader reader;

    @BeforeMethod
    public void setup() {
        dlm = mock(DistributedLogManager.class);
        reader = mock(AsyncLogReader.class);

        when(dlm.openAsyncLogReader(any(DLSN.class))).thenReturn(CompletableFuture.completedFuture(reader));
        when(dlm.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.readBulk(anyInt())).thenReturn(failedFuture(new EndOfStreamException("eos")));
        when(reader.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws IOException {
        if (dlm != null) {
            dlm.close();
        }
    }

    /**
     * Test Case: reader hits eos (end of stream)
     */
    @Test
    public void testReadEos() throws Exception {
        OutputStream outputStream = new ByteArrayOutputStream();
        try {
            DLInputStream.openReaderAsync(dlm)
                .thenCompose(d -> d.readAsync(outputStream))
                .thenCompose(DLInputStream::closeAsync).get();
        } catch (Exception e) {
            if (e.getCause() instanceof EndOfStreamException) {
                // no-op
            } else {
                fail(e.getMessage());
            }
        }

        verify(dlm, times(1)).openAsyncLogReader(eq(DLSN.InitialDLSN));
        verify(reader, times(1)).readBulk(eq(10));
        verify(reader, times(1)).asyncClose();
        verify(dlm, times(1)).asyncClose();
    }


    /**
     * Test Case: read records from the input stream. And output it to a output stream.
     */
    @Test
    public void testReadToOutputStream() {
        // prepare test data
        byte[] data = "test-read".getBytes();
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        List<LogRecordWithDLSN> records = new ArrayList<LogRecordWithDLSN>();
        records.add(record);

        when(record.getPayload()).thenReturn(data);
        when(reader.readBulk(anyInt()))
            .thenReturn(CompletableFuture.completedFuture(records))
            .thenReturn(failedFuture(new EndOfStreamException("eos")));


        // test code
        OutputStream outputStream = new ByteArrayOutputStream();
        try {
            DLInputStream.openReaderAsync(dlm)
                .thenCompose(d -> d.readAsync(outputStream))
                .thenCompose(DLInputStream::closeAsync).get();
        } catch (Exception e) {
            if (e.getCause() instanceof EndOfStreamException) {
                // no-op
            } else {
                fail(e.getMessage());
            }
        }

        byte[] result = ((ByteArrayOutputStream) outputStream).toByteArray();
        assertEquals("test-read", new String(result));

    }

    @Test
    public void openAsyncLogReaderFailed() {
        when(dlm.openAsyncLogReader(any(DLSN.class))).thenReturn(failedFuture(new Exception("Open reader was failed")));

        try {
            DLInputStream.openReaderAsync(dlm).get();
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(), "Open reader was failed");
        }
    }

    private <T> CompletableFuture<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(throwable);
        return completableFuture;
    }
}
