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
package org.apache.pulsar.functions.worker.dlog;

import static com.google.common.base.Charsets.UTF_8;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.exceptions.EndOfStreamException;
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
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        when(dlm.getInputStream(any(DLSN.class))).thenReturn(reader);
        when(reader.readNext(anyBoolean())).thenThrow(new EndOfStreamException("eos"));

        byte[] b = new byte[1];
        DLInputStream in = new DLInputStream(dlm);
        assertEquals("Should return 0 when reading an empty eos stream",
            0, in.read(b, 0, 1));
        assertEquals("Should return -1 when reading an empty eos stream",
            -1, in.read(b, 0, 1));
    }

    /**
     * Test Case: close the input stream
     */
    @Test
    public void testClose() throws Exception {
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        when(dlm.getInputStream(any(DLSN.class))).thenReturn(reader);

        DLInputStream in = new DLInputStream(dlm);
        verify(dlm, times(1)).getInputStream(eq(DLSN.InitialDLSN));
        in.close();
        verify(dlm, times(1)).close();
        verify(reader, times(1)).close();
    }

    /**
     * Test Case: read records from the input stream.
     */
    @Test
    public void testRead() throws Exception {
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        when(dlm.getInputStream(any(DLSN.class))).thenReturn(reader);

        byte[] data = "test-read".getBytes(UTF_8);
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        when(record.getPayLoadInputStream())
            .thenReturn(new ByteArrayInputStream(data));
        when(reader.readNext(anyBoolean()))
            .thenReturn(record)
            .thenThrow(new EndOfStreamException("eos"));

        DLInputStream in = new DLInputStream(dlm);
        int numReads = 0;
        int readByte;
        while ((readByte = in.read()) != -1) {
            assertEquals(data[numReads], readByte);
            ++numReads;
        }
        assertEquals(data.length, numReads);
    }

}
