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
package org.apache.pulsar.client.impl.conf;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.annotations.Test;

/**
 * Unit test {@link ConfigurationDataUtils}.
 */
public class ConfigurationDataUtilsTest {

    @Test
    public void testLoadClientConfigurationData() {
        ClientConfigurationData confData = new ClientConfigurationData();
        confData.setServiceUrl("pulsar://unknown:6650");
        confData.setMaxLookupRequest(600);
        confData.setMaxLookupRedirects(10);
        confData.setNumIoThreads(33);
        Map<String, Object> config = new HashMap<>();
        config.put("serviceUrl", "pulsar://localhost:6650");
        config.put("maxLookupRequest", 70000);
        config.put("maxLookupRedirects", 50);
        confData = ConfigurationDataUtils.loadData(config, confData, ClientConfigurationData.class);
        assertEquals("pulsar://localhost:6650", confData.getServiceUrl());
        assertEquals(70000, confData.getMaxLookupRequest());
        assertEquals(50, confData.getMaxLookupRedirects());
        assertEquals(33, confData.getNumIoThreads());
    }

    @Test
    public void testLoadProducerConfigurationData() {
        ProducerConfigurationData confData = new ProducerConfigurationData();
        confData.setProducerName("unset");
        confData.setBatchingEnabled(true);
        confData.setBatchingMaxMessages(1234);
        confData.setAutoUpdatePartitionsIntervalSeconds(1, TimeUnit.MINUTES);
        Map<String, Object> config = new HashMap<>();
        config.put("producerName", "test-producer");
        config.put("batchingEnabled", false);
        confData.setBatcherBuilder(BatcherBuilder.DEFAULT);
        confData = ConfigurationDataUtils.loadData(config, confData, ProducerConfigurationData.class);
        assertEquals("test-producer", confData.getProducerName());
        assertFalse(confData.isBatchingEnabled());
        assertEquals(1234, confData.getBatchingMaxMessages());
        assertEquals(60,confData.getAutoUpdatePartitionsIntervalSeconds());
    }

    @Test
    public void testLoadConsumerConfigurationData() {
        ConsumerConfigurationData confData = new ConsumerConfigurationData();
        confData.setSubscriptionName("unknown-subscription");
        confData.setPriorityLevel(10000);
        confData.setConsumerName("unknown-consumer");
        confData.setAutoUpdatePartitionsIntervalSeconds(1, TimeUnit.MINUTES);
        Map<String, Object> config = new HashMap<>();
        config.put("subscriptionName", "test-subscription");
        config.put("priorityLevel", 100);
        confData = ConfigurationDataUtils.loadData(config, confData, ConsumerConfigurationData.class);
        assertEquals("test-subscription", confData.getSubscriptionName());
        assertEquals(100, confData.getPriorityLevel());
        assertEquals("unknown-consumer", confData.getConsumerName());
        assertEquals(60,confData.getAutoUpdatePartitionsIntervalSeconds());
    }

    @Test
    public void testLoadReaderConfigurationData() {
        ReaderConfigurationData confData = new ReaderConfigurationData();
        confData.setTopicName("unknown");
        confData.setReceiverQueueSize(1000000);
        confData.setReaderName("unknown-reader");
        Map<String, Object> config = new HashMap<>();
        config.put("topicName", "test-topic");
        config.put("receiverQueueSize", 100);
        confData = ConfigurationDataUtils.loadData(config, confData, ReaderConfigurationData.class);
        assertEquals("test-topic", confData.getTopicName());
        assertEquals(100, confData.getReceiverQueueSize());
        assertEquals("unknown-reader", confData.getReaderName());
    }

    @Test
    public void testLoadConfigurationDataWithUnknownFields() {
        ReaderConfigurationData confData = new ReaderConfigurationData();
        confData.setTopicName("unknown");
        confData.setReceiverQueueSize(1000000);
        confData.setReaderName("unknown-reader");
        Map<String, Object> config = new HashMap<>();
        config.put("unknown", "test-topic");
        config.put("receiverQueueSize", 100);
        try {
            ConfigurationDataUtils.loadData(config, confData, ReaderConfigurationData.class);
            fail("Should fail loading configuration data with unknown fields");
        } catch (RuntimeException re) {
            assertTrue(re.getCause() instanceof IOException);
        }
    }

    @Test
    public void testConfigBuilder() throws PulsarClientException {
        ClientConfigurationData clientConfig = new ClientConfigurationData();
        clientConfig.setServiceUrl("pulsar://unknown:6650");
        clientConfig.setStatsIntervalSeconds(80);

        PulsarClientImpl pulsarClient = new PulsarClientImpl(clientConfig);
        assertNotNull(pulsarClient, "Pulsar client built using config should not be null");

        assertEquals(pulsarClient.getConfiguration().getServiceUrl(), "pulsar://unknown:6650");
        assertEquals(pulsarClient.getConfiguration().getNumListenerThreads(), 1, "builder default not set properly");
        assertEquals(pulsarClient.getConfiguration().getStatsIntervalSeconds(), 80,
                "builder default should overrite if set explicitly");
    }
}
