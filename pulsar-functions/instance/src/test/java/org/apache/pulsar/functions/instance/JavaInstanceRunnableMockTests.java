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
package org.apache.pulsar.functions.instance;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.JavaInstanceRunnable.SinkException;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.ArgumentCaptor;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class JavaInstanceRunnableMockTests {
	
  public static final String EXCLAMATION_JAVA_CLASS =
	        "org.apache.pulsar.functions.api.examples.ExclamationFunction";
  
  private static ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

  private ProducerBuilderImpl<byte[]> mockProducerBuilder;
  private PulsarClientImpl mockPulsarClient;
  
  @SuppressWarnings("unchecked")
  @BeforeMethod
  private void init() { 
	  mockPulsarClient = mock(PulsarClientImpl.class);
	  mockProducerBuilder = mock(ProducerBuilderImpl.class);
	  when(mockProducerBuilder.blockIfQueueFull(true)).thenReturn(mockProducerBuilder);
	  when(mockProducerBuilder.enableBatching(true)).thenReturn(mockProducerBuilder);
	  when(mockProducerBuilder.batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)).thenReturn(mockProducerBuilder);
	  when(mockPulsarClient.newProducer()).thenReturn(mockProducerBuilder);
  }
  
  @Test
  public final void testWorkingFunction() throws InterruptedException, SinkException {
	  InstanceConfig config = createInstanceConfig(
				RandomStringSource.class.getName(), 
				PassThroughSink.class.getName());
	  
	  JavaInstanceRunnable spiedRunnable = spy(createRunnable(config));
	  Future<?> runner = executor.submit(spiedRunnable);
	  Thread.sleep(2 * 1000);
	  runner.cancel(true);
	  
	  ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
	  verify(spiedRunnable, atLeast(1)).sendOutputMessage(any(Record.class), captor.capture());
	  assertNotNull(captor.getValue());
	  assertEquals(((String)captor.getValue()).length(), 11);
  }
	
  @Test
  public void testSinkException() throws Exception {
	InstanceConfig config = createInstanceConfig(
			RandomStringSource.class.getName(), 
            BadSink.class.getName());
	
    JavaInstanceRunnable spiedRunnable = spy(createRunnable(config));
    Future<?> runner = executor.submit(spiedRunnable);
	Thread.sleep(2 * 1000);
	runner.cancel(true);
    
    verify(spiedRunnable, times(1)).close();
    Object throwable = Whitebox.getInternalState(spiedRunnable, "deathException");
    assertNotNull(throwable);
    assertTrue(throwable instanceof SinkException);
  }
  
  private JavaInstanceRunnable createRunnable(InstanceConfig config) {
      JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
              config, mockPulsarClient, null, null, null, null, 
              Thread.currentThread().getContextClassLoader());
      return javaInstanceRunnable;
  }
  
  private static InstanceConfig createInstanceConfig(String srcClassName, String sinkClassName) {
	  InstanceConfig instanceConfig = new InstanceConfig();
	  FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
	  
	  functionDetailsBuilder.setSource(
		SourceSpec.newBuilder().setClassName(srcClassName).build());
	  
	  // The function itself
	  functionDetailsBuilder.setClassName(EXCLAMATION_JAVA_CLASS);
	  
	  // Output handler
	  functionDetailsBuilder.setSink(
		 SinkSpec.newBuilder().setClassName(sinkClassName).build());
	  
	  functionDetailsBuilder.setTenant("test-tenant");
	  functionDetailsBuilder.setNamespace("test-ns");
	  functionDetailsBuilder.setName("test-function");
	  
	  instanceConfig.setFunctionDetails(functionDetailsBuilder.build());
	  instanceConfig.setInstanceId(0);
	  instanceConfig.setClusterName("test-cluster");
	  instanceConfig.setMaxBufferedTuples(1024);
	  return instanceConfig;
  }
  
  private static class PassThroughSink implements Sink<String> {

	@Override
	public void close() throws Exception {
		
	}

	@Override
	public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
		
	}

	@Override
	public void write(Record<String> record) throws Exception {
		
	}
	  
  }
}
