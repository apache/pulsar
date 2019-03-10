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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.kafka.compat.PulsarClientKafkaConfig;
import org.apache.pulsar.client.kafka.compat.PulsarProducerKafkaConfig;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@PrepareForTest({PulsarClientKafkaConfig.class, PulsarProducerKafkaConfig.class})
@PowerMockIgnore({"org.apache.logging.log4j.*"})
public class PulsarKafkaProducerTest {

    @ObjectFactory
    // Necessary to make PowerMockito.mockStatic work with TestNG.
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test
    public void testPulsarKafkaProducer() {
        ClientBuilder mockClientBuilder = mock(ClientBuilder.class);
        ProducerBuilder mockProducerBuilder = mock(ProducerBuilder.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Assert.assertEquals((int)invocation.getArguments()[0], 1000000, "Send time out is suppose to be 1000.");
                return mockProducerBuilder;
            }
        }).when(mockProducerBuilder).sendTimeout(anyInt(), any(TimeUnit.class));
        doReturn(mockClientBuilder).when(mockClientBuilder).serviceUrl(anyString());
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Assert.assertEquals((int)invocation.getArguments()[0], 1000, "Keep alive interval is suppose to be 1000.");
                return mockClientBuilder;
            }
        }).when(mockClientBuilder).keepAliveInterval(anyInt(), any(TimeUnit.class));

        PowerMockito.mockStatic(PulsarClientKafkaConfig.class);
        PowerMockito.mockStatic(PulsarProducerKafkaConfig.class);
        when(PulsarClientKafkaConfig.getClientBuilder(any(Properties.class))).thenReturn(mockClientBuilder);
        when(PulsarProducerKafkaConfig.getProducerBuilder(any(PulsarClient.class), any(Properties.class))).thenReturn(mockProducerBuilder);

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList("pulsar://localhost:6650"));
        properties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "1000000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000000");

        PulsarKafkaProducer<String, String> pulsarKafkaProducer = new PulsarKafkaProducer<>(properties, null, null);

        verify(mockClientBuilder, times(1)).keepAliveInterval(1000, TimeUnit.SECONDS);
        verify(mockProducerBuilder, times(1)).sendTimeout(1000000, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid value 2147483648000 for 'connections.max.idle.ms'. Please use a value smaller than 2147483647000 milliseconds.")
    public void testPulsarKafkaProducerKeepAliveIntervalIllegalArgumentException() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList("pulsar://localhost:6650"));
        properties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, Long.toString((Integer.MAX_VALUE + 1L) * 1000));

        new PulsarKafkaProducer<>(properties, null, null);
    }

}
