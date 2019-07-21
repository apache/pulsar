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

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.testng.annotations.Test;

/**
 * Unit test of {@link DLOutputStream}.
 */
public class DLOutputStreamTest {

    /**
     * Test Case: close output stream.
     */
    @Test
    public void testClose() throws Exception {
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        AppendOnlyStreamWriter writer = mock(AppendOnlyStreamWriter.class);
        DLOutputStream out = new DLOutputStream(dlm, writer);

        out.close();
        verify(writer, times(1)).markEndOfStream();
        verify(writer, times(1)).close();
        verify(dlm, times(1)).close();
    }

    /**
     * Test Case: flush should force writing the data.
     */
    @Test
    public void testFlush() throws Exception {
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        AppendOnlyStreamWriter writer = mock(AppendOnlyStreamWriter.class);
        DLOutputStream out = new DLOutputStream(dlm, writer);

        out.flush();
        verify(writer, times(1)).force(eq(false));
    }

    /**
     * Test Case: test writing the data.
     */
    @Test
    public void testWrite() throws Exception {
        DistributedLogManager dlm = mock(DistributedLogManager.class);
        AppendOnlyStreamWriter writer = mock(AppendOnlyStreamWriter.class);
        DLOutputStream out = new DLOutputStream(dlm, writer);

        byte[] data = new byte[16];
        out.write(data);
        verify(writer, times(1)).write(data);
    }

}
