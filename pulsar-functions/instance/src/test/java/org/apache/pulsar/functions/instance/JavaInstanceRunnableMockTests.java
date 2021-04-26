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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.instance.JavaInstanceRunnable.SinkException;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class JavaInstanceRunnableMockTests {
	
  public static final String EXCLAMATION_JAVA_CLASS =
	        "org.apache.pulsar.functions.api.examples.ExclamationFunction";

  @SuppressWarnings("unchecked")
  private ProducerBuilderImpl<byte[]> mockProducerBuilder = mock(ProducerBuilderImpl.class);
	  
  private PulsarClientImpl mockPulsarClient = mock(PulsarClientImpl.class);
  
  @BeforeMethod
  private void init() { 
	  when(mockProducerBuilder.blockIfQueueFull(true)).thenReturn(mockProducerBuilder);
	  when(mockProducerBuilder.enableBatching(true)).thenReturn(mockProducerBuilder);
	  when(mockProducerBuilder.batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)).thenReturn(mockProducerBuilder);
	  when(mockPulsarClient.newProducer()).thenReturn(mockProducerBuilder);
  }
	
  @Test
  public void testSinkException() throws Exception {
    JavaInstanceRunnable spiedRunnable = spy(createRunnable());
    spiedRunnable.run();
    Thread.sleep(2 * 1000);
    
    verify(spiedRunnable, times(1)).close();
    Object throwable = ReflectionTestUtils.getField(spiedRunnable, "deathException");
    assertNotNull(throwable);
    assertTrue(throwable instanceof SinkException);
  }
  
  private JavaInstanceRunnable createRunnable() {
	  InstanceConfig config = createInstanceConfig();
      JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
              config, mockPulsarClient, null, null, null, null, 
              Thread.currentThread().getContextClassLoader());
      return javaInstanceRunnable;
  }
  
  private static InstanceConfig createInstanceConfig() {
	  InstanceConfig instanceConfig = new InstanceConfig();
	  FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
	  
	  functionDetailsBuilder.setSource(
		SourceSpec.newBuilder().setClassName(RandomStringSource.class.getName()).build());
	  
	  // The function itself
	  functionDetailsBuilder.setClassName(EXCLAMATION_JAVA_CLASS);
	  
	  // Output handler
	  functionDetailsBuilder.setSink(
		 SinkSpec.newBuilder().setClassName(BadSink.class.getName()).build());
	  
	  functionDetailsBuilder.setTenant("test-tenant");
	  functionDetailsBuilder.setNamespace("test-ns");
	  functionDetailsBuilder.setName("test-function");
	  
	  instanceConfig.setFunctionDetails(functionDetailsBuilder.build());
	  instanceConfig.setInstanceId(0);
	  instanceConfig.setClusterName("test-cluster");
	  instanceConfig.setMaxBufferedTuples(1024);
	  return instanceConfig;
  }
}
