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

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Unit test {@link ConfigurationDataUtils}.
 */
public class ConfigurationDataUtilsTest {

    @Test
    public void testLoadClientConfigurationData() {
        Map<String, Object> config = new HashMap<>();
        config.put("serviceUrl", "pulsar://localhost:6650");
        config.put("maxLookupRequest", 70000);
        ClientConfigurationData confData = ConfigurationDataUtils.loadData(config, ClientConfigurationData.class);
        assertEquals("pulsar://localhost:6650", confData.getServiceUrl());
        assertEquals(70000, confData.getMaxLookupRequest());
    }

    @Test
    public void testLoadProducerConfigurationData() {
        Map<String, Object> config = new HashMap<>();
        config.put("producerName", "test-producer");
        config.put("batchingEnabled", false);
        ProducerConfigurationData confData = ConfigurationDataUtils.loadData(config, ProducerConfigurationData.class);
        assertEquals("test-producer", confData.getProducerName());
        assertEquals(false, confData.isBatchingEnabled());
    }

    @Test
    public void testLoadConsumerConfigurationData() {
        Map<String, Object> config = new HashMap<>();
        config.put("subscriptionName", "test-subscription");
        config.put("priorityLevel", 100);
        ConsumerConfigurationData confData = ConfigurationDataUtils.loadData(config, ConsumerConfigurationData.class);
        assertEquals("test-subscription", confData.getSubscriptionName());
        assertEquals(100, confData.getPriorityLevel());
    }

    @Test
    public void testLoadReaderConfigurationData() {
        Map<String, Object> config = new HashMap<>();
        config.put("topicName", "test-topic");
        config.put("receiverQueueSize", 100);
        ReaderConfigurationData confData = ConfigurationDataUtils.loadData(config, ReaderConfigurationData.class);
        assertEquals("test-topic", confData.getTopicName());
        assertEquals(100, confData.getReceiverQueueSize());
    }

}
