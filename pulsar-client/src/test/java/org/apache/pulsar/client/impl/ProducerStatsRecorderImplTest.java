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
package org.apache.pulsar.client.impl;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Unit tests of {@link ProducerStatsRecorderImpl}.
 */
public class ProducerStatsRecorderImplTest {

    @Test
    public void testIncrementNumAcksReceived() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setStatsIntervalSeconds(1);
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.getConfiguration()).thenReturn(conf);
        Timer timer = new HashedWheelTimer();
        when(client.timer()).thenReturn(timer);
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        when(producer.getTopic()).thenReturn("topic-test");
        when(producer.getProducerName()).thenReturn("producer-test");
        when(producer.getPendingQueueSize()).thenReturn(1);
        ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        ProducerStatsRecorderImpl recorder = new ProducerStatsRecorderImpl(client, producerConfigurationData, producer);
        long latencyNs = TimeUnit.SECONDS.toNanos(1);
        recorder.incrementNumAcksReceived(latencyNs);
        Thread.sleep(1200);
        assertEquals(1000.0, recorder.getSendLatencyMillisMax(), 0.5);
    }

    @Test
    public void testGetStatsAndCancelStatsTimeoutWithoutArriveUpdateInterval() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setStatsIntervalSeconds(60);
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.getConfiguration()).thenReturn(conf);
        Timer timer = new HashedWheelTimer();
        when(client.timer()).thenReturn(timer);
        ProducerImpl<?> producer = mock(ProducerImpl.class);
        when(producer.getTopic()).thenReturn("topic-test");
        when(producer.getProducerName()).thenReturn("producer-test");
        when(producer.getPendingQueueSize()).thenReturn(1);
        ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        ProducerStatsRecorderImpl recorder = new ProducerStatsRecorderImpl(client, producerConfigurationData, producer);
        long latencyNs = TimeUnit.SECONDS.toNanos(1);
        recorder.incrementNumAcksReceived(latencyNs);
        recorder.cancelStatsTimeout();
        assertEquals(1000.0, recorder.getSendLatencyMillisMax(), 0.5);
    }
}
