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
package org.apache.pulsar.broker.service;

import java.util.Properties;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
        ClientConfigurationData clientConf = pulsar.getAdminClient().getClientConfigData();
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
        ClusterData data = new ClusterData("http://localhost:8080");

        // Set the configs and set some configs that won't apply
        Properties config = pulsar.getConfiguration().getProperties();
        config.setProperty("brokerClient_operationTimeoutMs", "60000");
        config.setProperty("brokerClient_statsIntervalSeconds", "10");

        PulsarClientImpl client = (PulsarClientImpl) pulsar.getBrokerService()
                .getReplicationClient("test");
        ClientConfigurationData clientConf = client.getConfiguration();
        Assert.assertEquals(clientConf.getOperationTimeoutMs(), 60000);
        // Config should override internal default, which is 0.
        Assert.assertEquals(clientConf.getStatsIntervalSeconds(), 10);
    }

    @Test
    public void testNamespaceServicePulsarClientConfiguration() {
        // This data only needs to have the service url for this test.
        ClusterData data = new ClusterData("http://localhost:8080");

        // Set the configs and set some configs that won't apply
        Properties config = pulsar.getConfiguration().getProperties();
        config.setProperty("brokerClient_operationTimeoutMs", "60000");
        config.setProperty("brokerClient_statsIntervalSeconds", "10");

        PulsarClientImpl client = pulsar.getNamespaceService().getNamespaceClient(data);
        ClientConfigurationData clientConf = client.getConfiguration();
        Assert.assertEquals(clientConf.getOperationTimeoutMs(), 60000);
        // Config should override internal default, which is 0.
        Assert.assertEquals(clientConf.getStatsIntervalSeconds(), 10);
    }

}
