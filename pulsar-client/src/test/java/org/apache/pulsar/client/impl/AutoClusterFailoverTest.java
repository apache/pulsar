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

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

public class AutoClusterFailoverTest {
    @Test
    public void testBuildAutoClusterFailoverInstance() {
        String primary = "pulsar://localhost:6650";
        String secondary = "pulsar://localhost:6651";
        long failoverDelay = 30;
        long switchBackDelay = 60;
        ServiceUrlProvider provider = AutoClusterFailover.builder()
                .primary(primary)
                .secondary(secondary)
                .failoverDelay(failoverDelay, TimeUnit.SECONDS)
                .switchBackDelay(switchBackDelay, TimeUnit.SECONDS)
                .build();

        AutoClusterFailover autoClusterFailover = (AutoClusterFailover) provider;
        Assert.assertTrue(provider instanceof AutoClusterFailover);
        Assert.assertEquals(primary, provider.getServiceUrl());
        Assert.assertEquals(primary, autoClusterFailover.getPrimary());
        Assert.assertEquals(secondary, autoClusterFailover.getSecondary());
        Assert.assertEquals(TimeUnit.SECONDS.toNanos(failoverDelay), autoClusterFailover.getFailoverDelayNs());
        Assert.assertEquals(TimeUnit.SECONDS.toNanos(switchBackDelay), autoClusterFailover.getSwitchBackDelayNs());
    }

    @Test
    public void testAutoClusterFailoverSwitch() {
        String primary = "pulsar://localhost:6650";
        String secondary = "pulsar://localhost:6651";
        long failoverDelay = 0;
        long switchBackDelay = 0;
        ServiceUrlProvider provider = AutoClusterFailover.builder()
                .primary(primary)
                .secondary(secondary)
                .failoverDelay(failoverDelay, TimeUnit.SECONDS)
                .switchBackDelay(switchBackDelay, TimeUnit.SECONDS)
                .build();

        AutoClusterFailover autoClusterFailover = Mockito.spy((AutoClusterFailover) provider);
        PulsarClient pulsarClient = PowerMockito.mock(PulsarClientImpl.class);
        Mockito.doReturn(false).when(autoClusterFailover).probeAvailable(primary);
        Mockito.doReturn(true).when(autoClusterFailover).probeAvailable(secondary);
        Mockito.doReturn(1_000).when(autoClusterFailover).getInterval();

        autoClusterFailover.initialize(pulsarClient);

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(secondary, autoClusterFailover.getServiceUrl()));

        Mockito.doReturn(true).when(autoClusterFailover).probeAvailable(primary);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(primary, autoClusterFailover.getServiceUrl()));
    }
}
