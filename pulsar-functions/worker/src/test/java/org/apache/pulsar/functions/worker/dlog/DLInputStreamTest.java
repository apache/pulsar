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
package org.apache.pulsar.functions.worker.dlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.testng.annotations.Test;

/**
 * Unit test of {@link DLInputStream}.
 */
public class DLInputStreamTest {

    private static DistributedLogManager mockDlm(AsyncLogReader reader) {
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        when(dlm.getStreamName()).thenReturn("test-stream");
        when(dlm.openAsyncLogReader(any(DLSN.class))).thenReturn(CompletableFuture.completedFuture(reader));
        return dlm;
    }

    private static AsyncLogReader mockReader() {
        AsyncLogReader reader = mock(AsyncLogReader.class);
        when(reader.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        return reader;
    }

    private static LogRecordWithDLSN mockRecord(byte[] payload) {
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        when(record.getPayLoadInputStream()).thenReturn(new ByteArrayInputStream(payload));
        return record;
    }

    /**
     * Test Case: reader hits eos (end of stream).
     */
    @Test
    public void testReadEos() throws Exception {
        AsyncLogReader reader = mockReader();
        DistributedLogManager dlm = mockDlm(reader);
        when(reader.readBulk(anyInt())).thenReturn(CompletableFuture.failedFuture(new EndOfStreamException("eos")));

        byte[] b = new byte[1];
        DLInputStream in = new DLInputStream(dlm);
        assertEquals("Should return -1 when reading an empty eos stream",
            -1, in.read(b, 0, 1));
        assertEquals("Should keep returning -1 when reading an empty eos stream",
            -1, in.read(b, 0, 1));
        assertEquals("Should return -1 when reading a byte from an empty eos stream",
            -1, in.read());
        // the end of stream is reached only once
        verify(reader, times(1)).readBulk(anyInt());
    }

    /**
     * Test Case: close the input stream.
     */
    @Test
    public void testClose() throws Exception {
        AsyncLogReader reader = mockReader();
        DistributedLogManager dlm = mockDlm(reader);

        DLInputStream in = new DLInputStream(dlm);
        verify(dlm, times(1)).openAsyncLogReader(eq(DLSN.InitialDLSN));
        in.close();
        verify(reader, times(1)).asyncClose();
        verify(dlm, times(1)).close();
    }

    /**
     * Test Case: read records from the input stream.
     */
    @Test
    public void testRead() throws Exception {
        AsyncLogReader reader = mockReader();
        DistributedLogManager dlm = mockDlm(reader);

        byte[] data = "test-read".getBytes(StandardCharsets.UTF_8);
        LogRecordWithDLSN record = mockRecord(data);
        when(reader.readBulk(anyInt()))
            .thenReturn(CompletableFuture.completedFuture(List.of(record)))
            .thenReturn(CompletableFuture.failedFuture(new EndOfStreamException("eos")));

        DLInputStream in = new DLInputStream(dlm);
        int numReads = 0;
        int readByte;
        while ((readByte = in.read()) != -1) {
            assertEquals(data[numReads], readByte);
            ++numReads;
        }
        assertEquals(data.length, numReads);
    }

    /**
     * Test Case: a read spans multiple records and batches of records.
     */
    @Test
    public void testReadAcrossRecords() throws Exception {
        AsyncLogReader reader = mockReader();
        DistributedLogManager dlm = mockDlm(reader);

        List<LogRecordWithDLSN> firstBatch = List.of(mockRecord("ab".getBytes(StandardCharsets.UTF_8)),
            mockRecord("cd".getBytes(StandardCharsets.UTF_8)));
        List<LogRecordWithDLSN> secondBatch = List.of(mockRecord("ef".getBytes(StandardCharsets.UTF_8)));
        when(reader.readBulk(anyInt()))
            .thenReturn(CompletableFuture.completedFuture(firstBatch))
            .thenReturn(CompletableFuture.completedFuture(secondBatch))
            .thenReturn(CompletableFuture.failedFuture(new EndOfStreamException("eos")));

        try (InputStream in = new DLInputStream(dlm)) {
            byte[] b = new byte[5];
            assertThat(in.read(b, 0, 5)).isEqualTo(5);
            assertThat(new String(b, StandardCharsets.UTF_8)).isEqualTo("abcde");
            assertThat(in.read(b, 0, 5)).isEqualTo(1);
            assertThat(b[0]).isEqualTo((byte) 'f');
            assertThat(in.read(b, 0, 5)).isEqualTo(-1);
        }
    }

    /**
     * Test Case: the stream never delivers the next records (for example an incomplete upload without an
     * end-of-stream marker). The read must fail after the read timeout instead of blocking forever.
     */
    @Test
    public void testReadTimesOut() throws Exception {
        AsyncLogReader reader = mockReader();
        DistributedLogManager dlm = mockDlm(reader);
        CompletableFuture<List<LogRecordWithDLSN>> neverCompleted = new CompletableFuture<>();
        when(reader.readBulk(anyInt())).thenReturn(neverCompleted);

        try (InputStream in = new DLInputStream(dlm, Duration.ofMillis(200))) {
            assertThatThrownBy(() -> in.read(new byte[1], 0, 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Timed out")
                .hasMessageContaining("test-stream")
                .hasCauseInstanceOf(TimeoutException.class);
        }
        assertThat(neverCompleted).isCancelled();
    }

    /**
     * Test Case: reading fails with the failure of the log stream read.
     */
    @Test
    public void testReadFailure() throws Exception {
        AsyncLogReader reader = mockReader();
        DistributedLogManager dlm = mockDlm(reader);
        when(reader.readBulk(anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new LogNotFoundException("deleted")));

        try (InputStream in = new DLInputStream(dlm)) {
            assertThatThrownBy(() -> in.read(new byte[1], 0, 1))
                .isInstanceOf(LogNotFoundException.class)
                .hasMessage("deleted");
        }
    }

    /**
     * Test Case: opening the reader fails.
     */
    @Test
    public void testOpenReaderFailure() {
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        when(dlm.getStreamName()).thenReturn("test-stream");
        when(dlm.openAsyncLogReader(any(DLSN.class)))
            .thenReturn(CompletableFuture.failedFuture(new LogNotFoundException("missing")));

        assertThatThrownBy(() -> new DLInputStream(dlm))
            .isInstanceOf(LogNotFoundException.class)
            .hasMessage("missing");
    }
}
