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
package org.apache.pulsar.functions.instance.producers;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link SimpleOneSinkTopicProducers}.
 */
public class SimpleOneSinkTopicProducersTest {

    private static final String TEST_SINK_TOPIC = "test-sink-topic";

    private PulsarClient mockClient;
    private Producer mockProducer;
    private SimpleOneSinkTopicProducers producers;

    @BeforeMethod
    public void setup() throws Exception {
        this.mockClient = mock(PulsarClient.class);
        this.mockProducer = mock(Producer.class);

        when(mockClient.createProducer(anyString(), any(ProducerConfiguration.class)))
            .thenReturn(mockProducer);

        this.producers = new SimpleOneSinkTopicProducers(mockClient, TEST_SINK_TOPIC);
    }

    @Test
    public void testInitializeClose() throws Exception {
        this.producers.initialize();

        verify(mockClient, times(1))
            .createProducer(eq(TEST_SINK_TOPIC), any(ProducerConfiguration.class));

        this.producers.close();

        verify(mockProducer, times(1)).close();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testGetProducerWithoutInitialization() throws Exception {
        this.producers.getProducer("test-src-topic", 0);
    }

    @Test
    public void testGetAndCloseProducer() throws Exception {
        this.producers.initialize();

        verify(mockClient, times(1))
            .createProducer(eq(TEST_SINK_TOPIC), any(ProducerConfiguration.class));

        assertSame(mockProducer, this.producers.getProducer("test-src-topic", 0));

        verify(mockClient, times(1))
            .createProducer(eq(TEST_SINK_TOPIC), any(ProducerConfiguration.class));

        producers.closeProducer("test-src-topic", 0);

        assertSame(mockProducer, producers.getProducer());

        verify(mockProducer, times(0)).close();
    }

}
