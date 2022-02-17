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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

@Test(groups = "broker-impl")
public class AutoClusterFailoverTest {
    @Test
    public void testBuildAutoClusterFailoverInstance() throws PulsarClientException {
        String primary = "pulsar://localhost:6650";
        String secondary = "pulsar://localhost:6651";
        long failoverDelay = 30;
        long switchBackDelay = 60;
        long checkInterval = 1_000;
        ServiceUrlProvider provider = AutoClusterFailover.builder()
                .primary(primary)
                .secondary(Collections.singletonList(secondary))
                .failoverDelay(failoverDelay, TimeUnit.SECONDS)
                .switchBackDelay(switchBackDelay, TimeUnit.SECONDS)
                .checkInterval(checkInterval, TimeUnit.MILLISECONDS)
                .build();

        AutoClusterFailover autoClusterFailover = (AutoClusterFailover) provider;
        Assert.assertTrue(provider instanceof AutoClusterFailover);
        Assert.assertEquals(primary, provider.getServiceUrl());
        Assert.assertEquals(primary, autoClusterFailover.getPrimary());
        Assert.assertEquals(secondary, autoClusterFailover.getSecondary().get(0));
        Assert.assertEquals(TimeUnit.SECONDS.toNanos(failoverDelay), autoClusterFailover.getFailoverDelayNs());
        Assert.assertEquals(TimeUnit.SECONDS.toNanos(switchBackDelay), autoClusterFailover.getSwitchBackDelayNs());
        Assert.assertEquals(checkInterval, autoClusterFailover.getIntervalMs());
        Assert.assertNull(autoClusterFailover.getPrimaryTlsTrustCertsFilePath());
        Assert.assertNull(autoClusterFailover.getPrimaryAuthentication());
        Assert.assertNull(autoClusterFailover.getSecondaryAuthentications());
        Assert.assertNull(autoClusterFailover.getSecondaryTlsTrustCertsFilePaths());

        String primaryTlsTrustCertsFilePath = "primary/path";
        String secondaryTlsTrustCertsFilePath = "primary/path";
        Authentication primaryAuthentication = AuthenticationFactory.create(
                "org.apache.pulsar.client.impl.auth.AuthenticationTls",
                "tlsCertFile:/path/to/primary-my-role.cert.pem,"
                        + "tlsKeyFile:/path/to/primary-my-role.key-pk8.pem");

        Authentication secondaryAuthentication = AuthenticationFactory.create(
                "org.apache.pulsar.client.impl.auth.AuthenticationTls",
                "tlsCertFile:/path/to/secondary-my-role.cert.pem,"
                        + "tlsKeyFile:/path/to/secondary-role.key-pk8.pem");
        Map<String, String> secondaryTlsTrustCertsFilePaths = new HashMap<>();
        secondaryTlsTrustCertsFilePaths.put(secondary, secondaryTlsTrustCertsFilePath);

        Map<String, Authentication> secondaryAuthentications = new HashMap<>();
        secondaryAuthentications.put(secondary, secondaryAuthentication);

        ServiceUrlProvider provider1 = AutoClusterFailover.builder()
                .primary(primary)
                .secondary(Collections.singletonList(secondary))
                .failoverDelay(failoverDelay, TimeUnit.SECONDS)
                .switchBackDelay(switchBackDelay, TimeUnit.SECONDS)
                .checkInterval(checkInterval, TimeUnit.MILLISECONDS)
                .secondaryTlsTrustCertsFilePath(secondaryTlsTrustCertsFilePaths)
                .secondaryAuthentication(secondaryAuthentications)
                .build();

        AutoClusterFailover autoClusterFailover1 = (AutoClusterFailover) provider1;
        Assert.assertEquals(secondaryTlsTrustCertsFilePath,
                autoClusterFailover1.getSecondaryTlsTrustCertsFilePaths().get(secondary));
        Assert.assertEquals(secondaryAuthentication, autoClusterFailover1.getSecondaryAuthentications().get(secondary));
    }

    @Test
    public void testAutoClusterFailoverSwitchWithoutAuthentication() {
        String primary = "pulsar://localhost:6650";
        String secondary = "pulsar://localhost:6651";
        long failoverDelay = 1;
        long switchBackDelay = 1;
        long checkInterval = 1_000;

        ClientConfigurationData configurationData = new ClientConfigurationData();

        ServiceUrlProvider provider = AutoClusterFailover.builder()
                .primary(primary)
                .secondary(Collections.singletonList(secondary))
                .failoverDelay(failoverDelay, TimeUnit.SECONDS)
                .switchBackDelay(switchBackDelay, TimeUnit.SECONDS)
                .checkInterval(checkInterval, TimeUnit.MILLISECONDS)
                .build();

        AutoClusterFailover autoClusterFailover = Mockito.spy((AutoClusterFailover) provider);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        Mockito.doReturn(false).when(autoClusterFailover).probeAvailable(primary);
        Mockito.doReturn(true).when(autoClusterFailover).probeAvailable(secondary);
        Mockito.doReturn(configurationData).when(pulsarClient).getConfiguration();

        autoClusterFailover.initialize(pulsarClient);

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(secondary, autoClusterFailover.getServiceUrl()));

        // primary cluster came back
        Mockito.doReturn(true).when(autoClusterFailover).probeAvailable(primary);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(primary, autoClusterFailover.getServiceUrl()));
    }

    @Test
    public void testAutoClusterFailoverSwitchWithAuthentication() throws IOException {
        String primary = "pulsar+ssl://localhost:6651";
        String secondary = "pulsar+ssl://localhost:6661";
        long failoverDelay = 1;
        long switchBackDelay = 1;
        long checkInterval = 1_000;
        String primaryTlsTrustCertsFilePath = "primary/path";
        String secondaryTlsTrustCertsFilePath = "primary/path";
        Authentication primaryAuthentication = AuthenticationFactory.create(
                "org.apache.pulsar.client.impl.auth.AuthenticationTls",
                "tlsCertFile:/path/to/primary-my-role.cert.pem,"
                        + "tlsKeyFile:/path/to/primary-my-role.key-pk8.pem");

        Authentication secondaryAuthentication = AuthenticationFactory.create(
                "org.apache.pulsar.client.impl.auth.AuthenticationTls",
                "tlsCertFile:/path/to/secondary-my-role.cert.pem,"
                        + "tlsKeyFile:/path/to/secondary-role.key-pk8.pem");

        Map<String, String> secondaryTlsTrustCertsFilePaths = new HashMap<>();
        secondaryTlsTrustCertsFilePaths.put(secondary, secondaryTlsTrustCertsFilePath);

        Map<String, Authentication> secondaryAuthentications = new HashMap<>();
        secondaryAuthentications.put(secondary, secondaryAuthentication);

        ClientConfigurationData configurationData = new ClientConfigurationData();
        configurationData.setTlsTrustCertsFilePath(primaryTlsTrustCertsFilePath);
        configurationData.setAuthentication(primaryAuthentication);

        ServiceUrlProvider provider = AutoClusterFailover.builder()
                .primary(primary)
                .secondary(Collections.singletonList(secondary))
                .checkInterval(checkInterval, TimeUnit.MILLISECONDS)
                .failoverDelay(failoverDelay, TimeUnit.SECONDS)
                .switchBackDelay(switchBackDelay, TimeUnit.SECONDS)
                .secondaryTlsTrustCertsFilePath(secondaryTlsTrustCertsFilePaths)
                .secondaryAuthentication(secondaryAuthentications)
                .build();

        AutoClusterFailover autoClusterFailover = Mockito.spy((AutoClusterFailover) provider);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        Mockito.doReturn(false).when(autoClusterFailover).probeAvailable(primary);
        Mockito.doReturn(true).when(autoClusterFailover).probeAvailable(secondary);
        Mockito.doReturn(configurationData).when(pulsarClient).getConfiguration();

        autoClusterFailover.initialize(pulsarClient);

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(secondary, autoClusterFailover.getServiceUrl()));
        Mockito.verify(pulsarClient, Mockito.atLeastOnce()).updateTlsTrustCertsFilePath(secondaryTlsTrustCertsFilePath);
        Mockito.verify(pulsarClient, Mockito.atLeastOnce()).updateAuthentication(secondaryAuthentication);

        // primary cluster came back
        Mockito.doReturn(true).when(autoClusterFailover).probeAvailable(primary);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(primary, autoClusterFailover.getServiceUrl()));
        Mockito.verify(pulsarClient, Mockito.atLeastOnce()).updateTlsTrustCertsFilePath(primaryTlsTrustCertsFilePath);
        Mockito.verify(pulsarClient, Mockito.atLeastOnce()).updateAuthentication(primaryAuthentication);

    }

    @Test
    public void testAutoClusterFailoverSwitchTlsTrustStore() throws IOException {
        String primary = "pulsar+ssl://localhost:6651";
        String secondary = "pulsar+ssl://localhost:6661";
        long failoverDelay = 1;
        long switchBackDelay = 1;
        long checkInterval = 1_000;
        String primaryTlsTrustStorePath = "primary/path";
        String secondaryTlsTrustStorePath = "secondary/path";
        String primaryTlsTrustStorePassword = "primaryPassword";
        String secondaryTlsTrustStorePassword = "secondaryPassword";

        Map<String, String> secondaryTlsTrustStorePaths = new HashMap<>();
        secondaryTlsTrustStorePaths.put(secondary, secondaryTlsTrustStorePath);
        Map<String, String> secondaryTlsTrustStorePasswords = new HashMap<>();
        secondaryTlsTrustStorePasswords.put(secondary, secondaryTlsTrustStorePassword);

        ClientConfigurationData configurationData = new ClientConfigurationData();
        configurationData.setTlsTrustStorePath(primaryTlsTrustStorePath);
        configurationData.setTlsTrustStorePassword(primaryTlsTrustStorePassword);

        ServiceUrlProvider provider = AutoClusterFailover.builder()
                .primary(primary)
                .secondary(Collections.singletonList(secondary))
                .failoverDelay(failoverDelay, TimeUnit.SECONDS)
                .switchBackDelay(switchBackDelay, TimeUnit.SECONDS)
                .checkInterval(checkInterval, TimeUnit.MILLISECONDS)
                .secondaryTlsTrustStorePath(secondaryTlsTrustStorePaths)
                .secondaryTlsTrustStorePassword(secondaryTlsTrustStorePasswords)
                .build();

        AutoClusterFailover autoClusterFailover = Mockito.spy((AutoClusterFailover) provider);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        Mockito.doReturn(false).when(autoClusterFailover).probeAvailable(primary);
        Mockito.doReturn(true).when(autoClusterFailover).probeAvailable(secondary);
        Mockito.doReturn(configurationData).when(pulsarClient).getConfiguration();

        autoClusterFailover.initialize(pulsarClient);

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(secondary, autoClusterFailover.getServiceUrl()));
        Mockito.verify(pulsarClient, Mockito.atLeastOnce())
                .updateTlsTrustStorePathAndPassword(secondaryTlsTrustStorePath, secondaryTlsTrustStorePassword);

        // primary cluster came back
        Mockito.doReturn(true).when(autoClusterFailover).probeAvailable(primary);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(primary, autoClusterFailover.getServiceUrl()));
        Mockito.verify(pulsarClient, Mockito.atLeastOnce())
                .updateTlsTrustStorePathAndPassword(primaryTlsTrustStorePath, primaryTlsTrustStorePassword);

    }
}
