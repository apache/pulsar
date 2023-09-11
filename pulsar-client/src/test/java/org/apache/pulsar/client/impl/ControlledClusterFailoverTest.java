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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.asynchttpclient.Request;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(groups = "broker-impl")
public class ControlledClusterFailoverTest {
    @Test
    public void testBuildControlledClusterFailoverInstance() throws IOException {
        String defaultServiceUrl = "pulsar://localhost:6650";
        String urlProvider = "http://localhost:8080/test";
        String keyA = "key-a";
        String valueA = "value-a";
        String keyB = "key-b";
        String valueB = "value-b";

        Map<String, String> header = new HashMap<>();
        header.put(keyA, valueA);
        header.put(keyB, valueB);
        ServiceUrlProvider provider = ControlledClusterFailover.builder()
            .defaultServiceUrl(defaultServiceUrl)
            .urlProvider(urlProvider)
            .urlProviderHeader(header)
            .build();

        ControlledClusterFailover controlledClusterFailover = (ControlledClusterFailover) provider;
        Request request = controlledClusterFailover.getRequestBuilder().build();

        Assert.assertTrue(provider instanceof ControlledClusterFailover);
        Assert.assertEquals(defaultServiceUrl, provider.getServiceUrl());
        Assert.assertEquals(defaultServiceUrl, controlledClusterFailover.getCurrentPulsarServiceUrl());
        Assert.assertEquals(urlProvider, request.getUri().toUrl());
        Assert.assertEquals(request.getHeaders().get(keyA), valueA);
        Assert.assertEquals(request.getHeaders().get(keyB), valueB);
    }

    @Test
    public void testControlledClusterFailoverSwitch() throws IOException {
        String defaultServiceUrl = "pulsar+ssl://localhost:6651";
        String backupServiceUrl = "pulsar+ssl://localhost:6661";
        String urlProvider = "http://localhost:8080";
        String tlsTrustCertsFilePath = "backup/path";
        String authPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationToken";
        String authParamsString = "token:xxxaaabbee";
        long interval = 1_000;

        ControlledClusterFailover.ControlledConfiguration controlledConfiguration =
                new ControlledClusterFailover.ControlledConfiguration();
        controlledConfiguration.setServiceUrl(backupServiceUrl);
        controlledConfiguration.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
        controlledConfiguration.setAuthPluginClassName(authPluginClassName);
        controlledConfiguration.setAuthParamsString(authParamsString);

        ServiceUrlProvider provider = ControlledClusterFailover.builder()
            .defaultServiceUrl(defaultServiceUrl)
            .urlProvider(urlProvider)
            .checkInterval(interval, TimeUnit.MILLISECONDS)
            .build();

        ControlledClusterFailover controlledClusterFailover = Mockito.spy((ControlledClusterFailover) provider);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);

        controlledClusterFailover.initialize(pulsarClient);

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(defaultServiceUrl, controlledClusterFailover.getServiceUrl()));

        Mockito.doReturn(controlledConfiguration).when(controlledClusterFailover)
                .fetchControlledConfiguration();
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(backupServiceUrl, controlledClusterFailover.getServiceUrl()));
        Mockito.verify(pulsarClient, Mockito.atLeastOnce())
                .updateServiceUrl(backupServiceUrl);
        Mockito.verify(pulsarClient, Mockito.atLeastOnce())
                .updateTlsTrustCertsFilePath(tlsTrustCertsFilePath);
        Mockito.verify(pulsarClient, Mockito.atLeastOnce())
                .updateAuthentication(Mockito.any(Authentication.class));

        // update controlled configuration
        String backupServiceUrlV1 = "pulsar+ssl://localhost:6662";
        String tlsTrustCertsFilePathV1 = "backup/pathV1";
        String authPluginClassNameV1 = "org.apache.pulsar.client.impl.auth.AuthenticationToken";
        String authParamsStringV1 = "token:xxxaaabbeev1";
        ControlledClusterFailover.ControlledConfiguration controlledConfiguration1 =
                new ControlledClusterFailover.ControlledConfiguration();
        controlledConfiguration1.setServiceUrl(backupServiceUrlV1);
        controlledConfiguration1.setTlsTrustCertsFilePath(tlsTrustCertsFilePathV1);
        controlledConfiguration1.setAuthPluginClassName(authPluginClassNameV1);
        controlledConfiguration1.setAuthParamsString(authParamsStringV1);
        Mockito.doReturn(controlledConfiguration1).when(controlledClusterFailover)
                .fetchControlledConfiguration();

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(backupServiceUrlV1, controlledClusterFailover.getServiceUrl()));
        Mockito.verify(pulsarClient, Mockito.atLeastOnce()).reloadLookUp();
        Mockito.verify(pulsarClient, Mockito.atLeastOnce()).updateServiceUrl(backupServiceUrlV1);
        Mockito.verify(pulsarClient, Mockito.atLeastOnce())
                .updateTlsTrustCertsFilePath(tlsTrustCertsFilePathV1);
        Mockito.verify(pulsarClient, Mockito.atLeastOnce())
                .updateAuthentication(Mockito.any(Authentication.class));

    }
}
