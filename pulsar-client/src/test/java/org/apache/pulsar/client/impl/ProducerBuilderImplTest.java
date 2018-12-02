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

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.mockito.Matchers;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

/**
 * Unit test of {@link ProducerBuilderImpl}.
 */
public class ProducerBuilderImplTest {

    private PulsarClientImpl client;
    private ProducerBuilderImpl producerBuilderImpl;

    @BeforeTest
    public void setup() {
        Producer producer = mock(Producer.class);
        client = mock(PulsarClientImpl.class);
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        when(client.newProducer()).thenReturn(producerBuilderImpl);

        when(client.createProducerAsync(
                Matchers.any(ProducerConfigurationData.class), Matchers.any(Schema.class), eq(null)))
                .thenReturn(CompletableFuture.completedFuture(producer));
        when(producer.sendAsync(anyString())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    public void testProducerBuilderImplWhenMessageRoutingModeIsCorrect() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        when(client.newProducer()).thenReturn(producerBuilderImpl);

        Producer producer = producerBuilderImpl.topic("testTopicName")
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        assertNotNull(producer);
    }

    @Test
    public void testProducerBuilderImplWhenMessageRoutingModeIsIncorrect() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        when(client.newProducer()).thenReturn(producerBuilderImpl);

        Producer producer = producerBuilderImpl.topic("testTopicName")
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .messageRouter(new CustomMessageRouter())
                .create();

        assertNotNull(producer);
    }

    @Test
    public void testProducerBuilderImplWhenMessageRoutingIsNotSetExplicitly() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        when(client.newProducer()).thenReturn(producerBuilderImpl);

        Producer producer = producerBuilderImpl.topic("testTopicName")
                .messageRouter(new CustomMessageRouter())
                .create();

        assertNotNull(producer);
    }

    private class CustomMessageRouter implements MessageRouter {
        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            int partitionIndex = Integer.parseInt(msg.getKey()) % metadata.numPartitions();
            return partitionIndex;
        }
    }
}
