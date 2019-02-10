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
package org.apache.pulsar.io.hdfs3.sink.seq;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertNotNull;

import org.apache.pulsar.io.hdfs3.sink.AbstractHdfsSinkTest;
import org.apache.pulsar.io.hdfs3.sink.seq.HdfsTextSink;
import org.testng.annotations.Test;

public class HdfsTextSinkTests extends AbstractHdfsSinkTest<String, String> {
    
    @Override
    protected void createSink() {
        sink = new HdfsTextSink();
    }

    @Test(enabled = false)
    public final void write100Test() throws Exception {
        map.put("filenamePrefix", "write100TestText-seq");
        map.put("fileExtension", ".seq");
        map.put("syncInterval", 1000);
        sink.open(map, mockSinkContext);
        
        assertNotNull(sink);
        assertNotNull(mockRecord);
        send(100);
        
        Thread.sleep(2000);
        verify(mockRecord, times(100)).ack();
        sink.close();
    }
    
    @Test(enabled = false)
    public final void write5000Test() throws Exception {
        map.put("filenamePrefix", "write5000TestText-seq");
        map.put("fileExtension", ".seq");
        map.put("syncInterval", 1000);
        sink.open(map, mockSinkContext);
        
        assertNotNull(sink);
        assertNotNull(mockRecord);
        send(5000);
        
        Thread.sleep(2000);
        verify(mockRecord, times(5000)).ack();
        sink.close();
    }
    
    @Test(enabled = false)
    public final void tenSecondTest() throws Exception {
        map.put("filenamePrefix", "tenSecondTestText-seq");
        map.put("fileExtension", ".seq");
        map.put("syncInterval", 1000);
        sink.open(map, mockSinkContext);
        
        assertNotNull(mockRecord);
        
        runFor(10); 
        sink.close();
    }
    
    @Test(enabled = false)
    public final void bzip2CompressionTest() throws Exception {
        map.put("filenamePrefix", "bzip2CompressionTestText-seq");
        map.put("compression", "BZIP2");
        map.remove("fileExtension");
        sink.open(map, mockSinkContext);
        
        assertNotNull(mockRecord);
        
        send(5000);
        verify(mockRecord, times(5000)).ack();
    }
    
    @Test(enabled = false)
    public final void deflateCompressionTest() throws Exception {
        map.put("filenamePrefix", "deflateCompressionTestText-seq");
        map.put("compression", "DEFLATE");
        map.remove("fileExtension");
        sink.open(map, mockSinkContext);
        
        assertNotNull(mockRecord);
        send(5000);
        verify(mockRecord, times(5000)).ack();
    }
}
