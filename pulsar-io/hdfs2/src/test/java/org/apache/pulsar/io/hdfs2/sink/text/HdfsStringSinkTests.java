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
package org.apache.pulsar.io.hdfs2.sink.text;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.apache.pulsar.io.hdfs2.sink.AbstractHdfsSinkTest;
import org.testng.annotations.Test;

public class HdfsStringSinkTests extends AbstractHdfsSinkTest<String, String> {

    @Override
    protected void createSink() {
        sink = new HdfsStringSink();
    }

    @Test
    public final void write5000Test() throws Exception {
        map.put("filenamePrefix", "write5000Test");
        map.put("fileExtension", ".txt");
        map.put("separator", '\n');
        sink.open(map, mockSinkContext);
        send(5000);
        sink.close();
        verify(mockRecord, times(5000)).ack();
    }

    @Test
    public final void fiveByTwoThousandTest() throws Exception {
        map.put("filenamePrefix", "fiveByTwoThousandTest");
        map.put("fileExtension", ".txt");
        map.put("separator", '\n');
        sink.open(map, mockSinkContext);

        for (int idx = 1; idx < 6; idx++) {
            send(2000);
        }
        sink.close();
        verify(mockRecord, times(2000 * 5)).ack();
    }

    @Test
    public final void tenSecondTest() throws Exception {
        map.put("filenamePrefix", "tenSecondTest");
        map.put("fileExtension", ".txt");
        map.put("separator", '\n');
        sink.open(map, mockSinkContext);
        runFor(10);
        sink.close();
    }

    @Test
    public final void maxPendingRecordsTest() throws Exception {
        map.put("filenamePrefix", "maxPendingRecordsTest");
        map.put("fileExtension", ".txt");
        map.put("separator", '\n');
        map.put("maxPendingRecords", 500);
        sink.open(map, mockSinkContext);
        runFor(10);
        sink.close();
    }

    @Test
    public final void bzip2CompressionTest() throws Exception {
        map.put("filenamePrefix", "bzip2CompressionTest");
        map.put("compression", "BZIP2");
        map.remove("fileExtension");
        map.put("separator", '\n');
        sink.open(map, mockSinkContext);
        send(5000);
        sink.close();
        verify(mockRecord, times(5000)).ack();
    }

    @Test
    public final void deflateCompressionTest() throws Exception {
        map.put("filenamePrefix", "deflateCompressionTest");
        map.put("compression", "DEFLATE");
        map.put("fileExtension", ".deflate");
        map.put("separator", '\n');
        sink.open(map, mockSinkContext);
        send(50000);
        sink.close();
        verify(mockRecord, times(50000)).ack();
    }
}
