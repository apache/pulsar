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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.Test;

/**
 * Unit test of {@link DLInputStream}.
 */
public class DLInputStreamTest {

    /**
     * Test Case: reader hits eos (end of stream)
     */
    @Test
    public void testReadEos() throws Exception {
        // mock class
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        AsyncLogReader reader = mock(AsyncLogReader.class);

        when(dlm.openAsyncLogReader(any(DLSN.class))).thenReturn(CompletableFuture.completedFuture(reader));
        when(dlm.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.readBulk(anyInt())).thenReturn(FutureUtil.failedFuture(new EndOfStreamException("eos")));
        when(reader.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));

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
        // mock classes
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        AsyncLogReader reader = mock(AsyncLogReader.class);

        when(dlm.openAsyncLogReader(any(DLSN.class))).thenReturn(CompletableFuture.completedFuture(reader));
        when(dlm.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.readBulk(anyInt())).thenReturn(FutureUtil.failedFuture(new EndOfStreamException("eos")));
        when(reader.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));

        // prepare test data
        byte[] data = "test-read".getBytes();
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        List<LogRecordWithDLSN> records = new ArrayList<LogRecordWithDLSN>();
        records.add(record);

        when(record.getPayload()).thenReturn(data);
        when(reader.readBulk(anyInt()))
            .thenReturn(CompletableFuture.completedFuture(records))
            .thenReturn(FutureUtil.failedFuture(new EndOfStreamException("eos")));


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

    /**
     * Test Case: read records from the input stream. And output it to a byte array.
     */
    @Test
    public void testReadToByteArray() throws Exception {
        // mock classes
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        AsyncLogReader reader = mock(AsyncLogReader.class);

        when(dlm.openAsyncLogReader(any(DLSN.class))).thenReturn(CompletableFuture.completedFuture(reader));
        when(dlm.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.readBulk(anyInt())).thenReturn(FutureUtil.failedFuture(new EndOfStreamException("eos")));
        when(reader.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));

        // prepare test data
        byte[] data = "test-read".getBytes();
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        List<LogRecordWithDLSN> records = new ArrayList<LogRecordWithDLSN>();
        records.add(record);

        when(record.getPayload()).thenReturn(data);
        when(reader.readBulk(anyInt()))
            .thenReturn(CompletableFuture.completedFuture(records))
            .thenReturn(FutureUtil.failedFuture(new EndOfStreamException("eos")));


        // test code
        byte[] result = new byte[0];
        try {
            result = DLInputStream.openReaderAsync(dlm)
                .thenCompose(DLInputStream::readAsync)
                .thenCompose(DLInputStream.ByteResult::getResult).get();
        } catch (Exception e) {
            if (e.getCause() instanceof EndOfStreamException) {
                // no-op
            } else {
                fail(e.getMessage());
            }
        }

        assertEquals("test-read", new String(result));
    }
}
