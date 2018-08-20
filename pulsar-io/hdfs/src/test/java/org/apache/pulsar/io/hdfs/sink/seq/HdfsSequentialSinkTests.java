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
package org.apache.pulsar.io.hdfs.sink.seq;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.hdfs.sink.seq.HdfsAbstractSequenceFileSink;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class HdfsSequentialSinkTests {

	@Mock
	private SinkContext mockSinkContext;
	
	@Mock
	private Record<String> mockRecord;
	
	private Map<String, Object> map;
	private HdfsAbstractSequenceFileSink<Long, String, LongWritable, Text> sink;
	
	@SuppressWarnings("unchecked")
	@Before
	public final void setUp() throws Exception {
		map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "../pulsar-io/hdfs/src/test/resources/hadoop/core-site.xml,"
				+ "../pulsar-io/hdfs/src/test/resources/hadoop/hdfs-site.xml");
		map.put("directory", "/tmp/testing");
		map.put("filenamePrefix", "prefix");
		
		mockSinkContext = mock(SinkContext.class);
		
		mockRecord = mock(Record.class);
		when(mockRecord.getRecordSequence()).thenAnswer(new Answer<Optional<Long>>() {
		  long sequenceCounter = 0;
          public Optional<Long> answer(InvocationOnMock invocation) throws Throwable {
             return Optional.of(sequenceCounter++);
          }});
		
		when(mockRecord.getValue()).thenAnswer(new Answer<String>() {
	          public String answer(InvocationOnMock invocation) throws Throwable {
	             return new String(UUID.randomUUID().toString());
	          }});
		
		sink = new HdfsSequentialTextSink();
	}
	
	@After
	public final void tearDown() throws Exception {
		sink.close();
	}
	
	@Test
	public final void write100Test() throws Exception {
		
		map.put("fileExtension", ".seq");
		map.put("syncInterval", 1000);
		sink.open(map, mockSinkContext);
		
		assertNotNull(sink);
		send(100);
		
		Thread.sleep(2000);
		verify(mockRecord, times(100)).ack();
	}
	
	@Test
	@Ignore
	public final void write5000Test() throws Exception {
		
		map.put("fileExtension", ".seq");
		map.put("syncInterval", 1000);
		sink.open(map, mockSinkContext);
		
		assertNotNull(sink);
		send(5000);
		
		Thread.sleep(2000);
		verify(mockRecord, times(5000)).ack();
	}
	
	@Test
	@Ignore
	public final void TenSecondTest() throws Exception {
		map.put("fileExtension", ".seq");
		map.put("syncInterval", 1000);
		sink.open(map, mockSinkContext);
		runFor(10);	
	}
	
	private final void send(int numRecords) {
		for (int idx = 0; idx < numRecords; idx++) {
			sink.write(mockRecord);
		}
	}
	
	private final void runFor(int numSeconds) throws InterruptedException {
		Producer producer = new Producer();
		producer.start();
		Thread.sleep(numSeconds * 1000); // Run for N seconds
		producer.halt();
		producer.join(2000);
	}
	
	private final class Producer extends Thread {
		public boolean keepRunning = true;
        @Override
        public void run() {
        	while (keepRunning)
               sink.write(mockRecord);
        }
        
        public void halt() { 
        	keepRunning = false; 
        }
        
	}
}
