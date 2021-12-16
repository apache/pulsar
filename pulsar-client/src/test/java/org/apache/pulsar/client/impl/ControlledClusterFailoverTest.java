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

import java.io.IOException;
import java.net.URL;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

public class ControlledClusterFailoverTest {
    @Test
    public void testBuildControlledClusterFailoverInstance() throws IOException {
        String defaultServiceUrl = "pulsar://localhost:6650";
        String urlProvider = "http://localhost:8080";

        ServiceUrlProvider provider = ControlledClusterFailover.builder()
                .defaultServiceUrl(defaultServiceUrl)
                .urlProvider(urlProvider)
                .build();

        ControlledClusterFailover controlledClusterFailover = (ControlledClusterFailover) provider;

        Assert.assertTrue(provider instanceof ControlledClusterFailover);
        Assert.assertEquals(defaultServiceUrl, provider.getServiceUrl());
        Assert.assertEquals(defaultServiceUrl, controlledClusterFailover.getCurrentPulsarServiceUrl());
        Assert.assertTrue(new URL(urlProvider).equals(controlledClusterFailover.getPulsarUrlProvider()));
    }

    @Test
    public void testControlledClusterFailoverSwitch() throws IOException {
        String defaultServiceUrl = "pulsar://localhost:6650";
        String backupServiceUrl = "pulsar://localhost:6651";
        String urlProvider = "http://localhost:8080";

        ServiceUrlProvider provider = ControlledClusterFailover.builder()
                .defaultServiceUrl(defaultServiceUrl)
                .urlProvider(urlProvider)
                .build();

        ControlledClusterFailover controlledClusterFailover = Mockito.spy((ControlledClusterFailover) provider);
        PulsarClient pulsarClient = PowerMockito.mock(PulsarClientImpl.class);
        Mockito.doReturn(1_000).when(controlledClusterFailover).getInterval();
        controlledClusterFailover.initialize(pulsarClient);

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(defaultServiceUrl, controlledClusterFailover.getServiceUrl()));

        Mockito.doReturn(backupServiceUrl).when(controlledClusterFailover).fetchServiceUrl();
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(backupServiceUrl, controlledClusterFailover.getServiceUrl()));

        Mockito.doReturn(defaultServiceUrl).when(controlledClusterFailover).fetchServiceUrl();
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(defaultServiceUrl, controlledClusterFailover.getServiceUrl()));

    }
}
