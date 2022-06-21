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

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.internal.PropertiesUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

public class PulsarClientConfigurationOverrideTest {
    @Test
    public void testFilterAndMapProperties() {
        // Create a default config
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.getProperties().setProperty("keepAliveIntervalSeconds", "15");
        conf.getProperties().setProperty("brokerClient_keepAliveIntervalSeconds", "25");

        // Apply the filtering and mapping logic
        Map<String, Object> result = PropertiesUtils.filterAndMapProperties(conf.getProperties(), "brokerClient_");

        // Ensure the results match expectations
        Assert.assertEquals(result.size(), 1, "The filtered map should have one entry.");
        Assert.assertNull(result.get("brokerClient_keepAliveIntervalSeconds"),
                "The mapped prop should not be in the result.");
        Assert.assertEquals(result.get("keepAliveIntervalSeconds"), "25", "The original value is overridden.");

        // Create sample ClientBuilder
        ClientBuilder builder = PulsarClient.builder();
        Assert.assertEquals(
                ((ClientBuilderImpl) builder).getClientConfigurationData().getKeepAliveIntervalSeconds(), 30);
        // Note: this test would fail if any @Secret fields were set before the loadConf and the accessed afterwards.
        builder.loadConf(result);
        Assert.assertEquals(
                ((ClientBuilderImpl) builder).getClientConfigurationData().getKeepAliveIntervalSeconds(), 25);
    }
}
