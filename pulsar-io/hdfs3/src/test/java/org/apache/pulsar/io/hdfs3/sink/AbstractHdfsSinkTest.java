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
package org.apache.pulsar.io.hdfs3.sink;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.hdfs3.sink.HdfsAbstractSink;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;

/**
 * Simple base class for all the HDFS sink test cases.
 * Provides utility methods for sending records to the sink.
 *
 */
public abstract class AbstractHdfsSinkTest<K, V> {
    
    @Mock
    protected SinkContext mockSinkContext;
    
    @Mock
    protected Record<V> mockRecord;
    
    protected Map<String, Object> map;
    protected HdfsAbstractSink<K, V> sink;
    
    @SuppressWarnings("unchecked")
    @BeforeMethod(alwaysRun = true)
    public final void setUp() throws Exception {
        map = new HashMap<String, Object> ();
        map.put("hdfsConfigResources", "../pulsar/pulsar-io/hdfs/src/test/resources/hadoop/core-site.xml,"
                + "../pulsar/pulsar-io/hdfs/src/test/resources/hadoop/hdfs-site.xml");
        map.put("directory", "/tmp/testing");
        map.put("filenamePrefix", "prefix");
        
        mockSinkContext = mock(SinkContext.class);
        
        mockRecord = mock(Record.class);
        when(mockRecord.getRecordSequence()).thenAnswer(new Answer<Optional<Long>>() {
          long sequenceCounter = 0;
          public Optional<Long> answer(InvocationOnMock invocation) throws Throwable {
             return Optional.of(sequenceCounter++);
          }});
        
        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            long sequenceCounter = 0;
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
               return Optional.of( "key-" + sequenceCounter++);
            }});
        
        when(mockRecord.getValue()).thenAnswer(new Answer<String>() {
            long sequenceCounter = 0;
            public String answer(InvocationOnMock invocation) throws Throwable {
                 return new String( "value-" + sequenceCounter++ + "-" + UUID.randomUUID().toString());
            }});
        
        createSink();
    }

    protected abstract void createSink();

    protected final void send(int numRecords) throws Exception {
        for (int idx = 0; idx < numRecords; idx++) {
            sink.write(mockRecord);
        }
    }
    
    protected final void runFor(int numSeconds) throws InterruptedException {
        Producer producer = new Producer();
        producer.start();
        Thread.sleep(numSeconds * 1000); // Run for N seconds
        producer.halt();
        producer.join(2000);
    }
    
    protected final class Producer extends Thread {
        public boolean keepRunning = true;
        @Override
        public void run() {
            while (keepRunning)
                try {
                    sink.write(mockRecord);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
        
        public void halt() { 
            keepRunning = false; 
        }
        
    }
}
