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
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.mockito.Matchers;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import junit.framework.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

/**
 * Unit tests of {@link ProducerBuilderImpl}.
 */
public class ProducerBuilderImplTest {

    private static final long BACKOFF_INTERVAL_IN_NANOSECONDS = TimeUnit.SECONDS.toNanos(2);
    private static final long MAX_BACKOFF_INTERVAL_IN_NANOSECONDS = TimeUnit.SECONDS.toNanos(40);
    private static final String TOPIC_NAME = "testTopicName";
    private PulsarClientImpl client;
    private ProducerBuilderImpl producerBuilderImpl;

    @BeforeTest
    public void setup() {
        Producer producer = mock(Producer.class);
        ClientConfigurationData clientConf = new ClientConfigurationData();

        client = mock(PulsarClientImpl.class);
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        when(client.newProducer()).thenReturn(producerBuilderImpl);
        clientConf.setDefaultBackoffIntervalNanos(BACKOFF_INTERVAL_IN_NANOSECONDS);
        clientConf.setMaxBackoffIntervalNanos(MAX_BACKOFF_INTERVAL_IN_NANOSECONDS);
        when(client.getConfiguration()).thenReturn(clientConf);

        when(client.createProducerAsync(
                Matchers.any(ProducerConfigurationData.class), Matchers.any(Schema.class), eq(null)))
                .thenReturn(CompletableFuture.completedFuture(producer));
    }

    @Test
    public void testCorrectBackoffConfiguration() { 
    	producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
    	try {
			ProducerImpl producer = (ProducerImpl) producerBuilderImpl.topic(TOPIC_NAME)
					 .producerName("Test-Producer-Configuration")
					 .create();
			Backoff backoff = producer.getConnectionHandler().backoff;
			Assert.assertEquals(backoff.backoffIntervalNanos(), BACKOFF_INTERVAL_IN_NANOSECONDS);
			Assert.assertEquals(backoff.maxBackoffIntervalNanos(), MAX_BACKOFF_INTERVAL_IN_NANOSECONDS);
		} catch (PulsarClientException e) { 
			// this will not happen
		}
    }

    @Test
    public void testProducerBuilderImpl() throws PulsarClientException {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key2", "Test-Value2");

        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        Producer producer = producerBuilderImpl.topic(TOPIC_NAME)
                .producerName("Test-Producer")
                .maxPendingMessages(2)
                .addEncryptionKey("Test-EncryptionKey")
                .property("Test-Key", "Test-Value")
                .properties(properties)
                .create();

        assertNotNull(producer);
    }

    @Test
    public void testProducerBuilderImplWhenMessageRoutingModeAndMessageRouterAreNotSet() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        Producer producer = producerBuilderImpl.topic(TOPIC_NAME)
                .create();
        assertNotNull(producer);
    }

    @Test
    public void testProducerBuilderImplWhenMessageRoutingModeIsSinglePartition() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        Producer producer = producerBuilderImpl.topic(TOPIC_NAME)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        assertNotNull(producer);
    }

    @Test
    public void testProducerBuilderImplWhenMessageRoutingModeIsRoundRobinPartition() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        Producer producer = producerBuilderImpl.topic(TOPIC_NAME)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();
        assertNotNull(producer);
    }

    @Test
    public void testProducerBuilderImplWhenMessageRoutingIsSetImplicitly() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        Producer producer = producerBuilderImpl.topic(TOPIC_NAME)
                .messageRouter(new CustomMessageRouter())
                .create();
        assertNotNull(producer);
    }

    @Test
    public void testProducerBuilderImplWhenMessageRoutingIsCustomPartition() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        Producer producer = producerBuilderImpl.topic(TOPIC_NAME)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new CustomMessageRouter())
                .create();
        assertNotNull(producer);
    }

    @Test(expectedExceptions = PulsarClientException.class)
    public void testProducerBuilderImplWhenMessageRoutingModeIsSinglePartitionAndMessageRouterIsSet()
            throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .messageRouter(new CustomMessageRouter())
                .create();
    }

    @Test(expectedExceptions = PulsarClientException.class)
    public void testProducerBuilderImplWhenMessageRoutingModeIsRoundRobinPartitionAndMessageRouterIsSet()
            throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .messageRouter(new CustomMessageRouter())
                .create();
    }

    @Test(expectedExceptions = PulsarClientException.class)
    public void testProducerBuilderImplWhenMessageRoutingModeIsCustomPartitionAndMessageRouterIsNotSet()
            throws PulsarClientException {
        ProducerBuilderImpl producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenTopicNameIsNull() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(null)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenTopicNameIsBlank() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic("   ")
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenProducerNameIsNull() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .producerName(null)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenProducerNameIsBlank() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .producerName("   ")
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenSendTimeoutIsNegative() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .producerName("Test-Producer")
                .sendTimeout(-1, TimeUnit.MILLISECONDS)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenMaxPendingMessagesIsNegative() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .producerName("Test-Producer")
                .maxPendingMessages(-1)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenEncryptionKeyIsNull() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .addEncryptionKey(null)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenEncryptionKeyIsBlank() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .addEncryptionKey("   ")
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertyKeyIsNull() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .property(null, "Test-Value")
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertyKeyIsBlank() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .property("   ", "Test-Value")
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertyValueIsNull() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .property("Test-Key", null)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertyValueIsBlank() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .property("Test-Key", "   ")
                .create();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testProducerBuilderImplWhenPropertiesIsNull() throws PulsarClientException {
        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .properties(null)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertiesKeyIsNull() throws PulsarClientException {
        Map<String, String> properties = new HashMap<>();
        properties.put(null, "Test-Value");

        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .properties(properties)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertiesKeyIsBlank() throws PulsarClientException {
        Map<String, String> properties = new HashMap<>();
        properties.put("   ", "Test-Value");

        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .properties(properties)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertiesValueIsNull() throws PulsarClientException {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key", null);

        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .properties(properties)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertiesValueIsBlank() throws PulsarClientException {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key", "   ");

        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .properties(properties)
                .create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerBuilderImplWhenPropertiesIsEmpty() throws PulsarClientException {
        Map<String, String> properties = new HashMap<>();

        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);
        producerBuilderImpl.topic(TOPIC_NAME)
                .properties(properties)
                .create();
    }

    private class CustomMessageRouter implements MessageRouter {
        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            int partitionIndex = Integer.parseInt(msg.getKey()) % metadata.numPartitions();
            return partitionIndex;
        }
    }
}
