package org.apache.pulsar.io.hdfs.sink.text;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class HdfsStringSinkTests {

	@Mock
	private SinkContext mockSinkContext;
	
	@Mock
	private Record<String> mockRecord;
	
	private Map<String, Object> map;
	private HdfsAbstractTextFileSink<String, String> sink;
	
	@SuppressWarnings("unchecked")
	@Before
	public final void setUp() throws Exception {
		map = new HashMap<String, Object> ();
		map.put("hdfsConfigResources", "../pulsar-io/hdfs/src/test/resources/hadoop/core-site.xml,"
				+ "../pulsar-io/hdfs/src/test/resources/hadoop/hdfs-site.xml");
		map.put("directory", "/tmp/testing");
		map.put("filenamePrefix", "prefix");
		map.put("fileExtension", ".txt");
		map.put("separator", '\n');
		
		sink = new HdfsStringSink();
		mockSinkContext = mock(SinkContext.class);
		
		mockRecord = mock(Record.class);
		when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
          public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
             return Optional.of("");
          }});
		
		when(mockRecord.getValue()).thenAnswer(new Answer<String>() {
	          public String answer(InvocationOnMock invocation) throws Throwable {
	             return new String(UUID.randomUUID().toString());
	          }});
		
	}
	
	@After
	public final void tearDown() throws Exception {
		sink.close();
	}
	
	@Test
	@Ignore
	public final void write5000Test() throws Exception {
		sink.open(map, mockSinkContext);
		send(5000);
		verify(mockRecord, times(5000)).ack();
	}
	
	@Test
	@Ignore
	public final void FiveByTwoThousandTest() throws Exception {
		sink.open(map, mockSinkContext);
		
		for (int idx = 1; idx < 6; idx++) {
			send(2000);
			verify(mockRecord, times(2000 * idx)).ack();
		}
	}
	
	@Test
	@Ignore
	public final void TenSecondTest() throws Exception {
		sink.open(map, mockSinkContext);
		runFor(10);	
	}
	
	@Test
	@Ignore
	public final void maxPendingRecordsTest() throws Exception {
		map.put("maxPendingRecords", 500);
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
