/*
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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import lombok.Cleanup;
import java.util.Optional;
import java.util.Properties;

public class BrokerInternalClientConfigurationOverrideTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPulsarServiceAdminClientConfiguration() throws PulsarServerException {
        Properties config = pulsar.getConfiguration().getProperties();
        config.setProperty("brokerClient_operationTimeoutMs", "60000");
        config.setProperty("brokerClient_statsIntervalSeconds", "10");
        ClientConfigurationData clientConf = ((PulsarAdminImpl) pulsar.getAdminClient()).getClientConfigData();
        Assert.assertEquals(clientConf.getOperationTimeoutMs(), 60000);
        Assert.assertEquals(clientConf.getStatsIntervalSeconds(), 10);
    }

    @Test
    public void testPulsarServicePulsarClientConfiguration() throws PulsarServerException {
        Properties config = pulsar.getConfiguration().getProperties();
        config.setProperty("brokerClient_operationTimeoutMs", "60000");
        config.setProperty("brokerClient_statsIntervalSeconds", "10");
        pulsar.getConfiguration().setBrokerClientAuthenticationParameters("sensitive");
        ClientConfigurationData clientConf = ((PulsarClientImpl) pulsar.getClient()).getConfiguration();
        Assert.assertEquals(clientConf.getOperationTimeoutMs(), 60000);
        // Config should override internal default, which is 0.
        Assert.assertEquals(clientConf.getStatsIntervalSeconds(), 10);
        Assert.assertEquals(clientConf.getAuthParams(), "sensitive");
    }

    @Test
    public void testBrokerServicePulsarClientConfiguration() {
        // This data only needs to have the service url for this test.
        ClusterData data = ClusterData.builder().serviceUrl("http://localhost:8080").build();

        // Set the configs and set some configs that won't apply
        Properties config = pulsar.getConfiguration().getProperties();
        config.setProperty("brokerClient_operationTimeoutMs", "60000");
        config.setProperty("brokerClient_statsIntervalSeconds", "10");
        config.setProperty("memoryLimitBytes", "10");
        config.setProperty("brokerClient_memoryLimitBytes", "100000");

        PulsarClientImpl client = (PulsarClientImpl) pulsar.getBrokerService()
                .getReplicationClient("an_arbitrary_name", Optional.of(data));
        ClientConfigurationData clientConf = client.getConfiguration();
        Assert.assertEquals(clientConf.getOperationTimeoutMs(), 60000);
        // Config should override internal default, which is 0.
        Assert.assertEquals(clientConf.getStatsIntervalSeconds(), 10);
        // This config defaults to 0 (for good reason), but it could be overridden by configuration.
        Assert.assertEquals(clientConf.getMemoryLimitBytes(), 100000);
    }

    @Test
    public void testNamespaceServicePulsarClientConfiguration() {
        // This data only needs to have the service url for this test.
        ClusterDataImpl data = (ClusterDataImpl) ClusterData.builder().serviceUrl("http://localhost:8080").build();

        // Set the configs and set some configs that won't apply
        Properties config = pulsar.getConfiguration().getProperties();
        config.setProperty("brokerClient_operationTimeoutMs", "60000");
        config.setProperty("brokerClient_statsIntervalSeconds", "10");
        config.setProperty("memoryLimitBytes", "10");
        config.setProperty("brokerClient_memoryLimitBytes", "100000");

        PulsarClientImpl client = pulsar.getNamespaceService().getNamespaceClient(data);
        ClientConfigurationData clientConf = client.getConfiguration();
        Assert.assertEquals(clientConf.getOperationTimeoutMs(), 60000);
        // Config should override internal default, which is 0.
        Assert.assertEquals(clientConf.getStatsIntervalSeconds(), 10);
        // This config defaults to 0 (for good reason), but it could be overridden by configuration.
        Assert.assertEquals(clientConf.getMemoryLimitBytes(), 100000);
    }

    @Test
    public void testOldNamespacePolicy() throws Exception {
        
        String ns = "prop/oldNsWithDefaultNonNullValues";
        String topic = "persistent://" + ns + "/t1";
        Policies policies = new Policies();
        policies.max_consumers_per_subscription = -1;
        policies.max_consumers_per_topic = -1;
        policies.max_producers_per_topic = -1;
        policies.max_subscriptions_per_topic = -1;
        policies.max_topics_per_namespace = -1;
        policies.max_unacked_messages_per_consumer = -1;
        policies.max_unacked_messages_per_subscription = -1;
        admin.namespaces().createNamespace(ns, policies);
        
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic).create();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        assertEquals(topicRef.topicPolicies.getMaxUnackedMessagesOnSubscription().get(),
                conf.getMaxUnackedMessagesPerSubscription());
        assertEquals(topicRef.topicPolicies.getMaxConsumersPerSubscription().get(),
                conf.getMaxConsumersPerSubscription());
        assertEquals(topicRef.topicPolicies.getMaxConsumerPerTopic().get(),
                conf.getMaxConsumersPerTopic());
        assertEquals(topicRef.topicPolicies.getMaxProducersPerTopic().get(),
                conf.getMaxProducersPerTopic());
        assertEquals(topicRef.topicPolicies.getMaxSubscriptionsPerTopic().get(),
                conf.getMaxSubscriptionsPerTopic());
        assertEquals(topicRef.topicPolicies.getTopicMaxMessageSize().get(),
                conf.getMaxMessageSize());
        assertEquals(topicRef.topicPolicies.getMaxUnackedMessagesOnConsumer().get(),
                conf.getMaxUnackedMessagesPerConsumer());
        
        
    }
}
