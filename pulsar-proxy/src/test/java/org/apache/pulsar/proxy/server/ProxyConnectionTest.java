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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

public class ProxyConnectionTest {

    @Test
    public void testMatchesHostAndPort() {
        assertTrue(ProxyConnection
                .matchesHostAndPort("pulsar://", "pulsar://1.2.3.4:6650", "1.2.3.4:6650"));
        assertTrue(ProxyConnection
                .matchesHostAndPort("pulsar+ssl://", "pulsar+ssl://1.2.3.4:6650", "1.2.3.4:6650"));
        assertFalse(ProxyConnection
                .matchesHostAndPort("pulsar://", "pulsar://1.2.3.4:12345", "5.6.7.8:1234"));
        assertFalse(ProxyConnection
                .matchesHostAndPort("pulsar://", "pulsar://1.2.3.4:12345", "1.2.3.4:1234"));
    }
    @Test
    public void testCreateClientConfiguration() {
        ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
        proxyConfiguration.setTlsEnabledWithBroker(true);
        String proxyUrlTls = "pulsar+ssl://proxy:6651";
        String proxyUrl = "pulsar://proxy:6650";

        ProxyService proxyService = mock(ProxyService.class);
        doReturn(proxyConfiguration).when(proxyService).getConfiguration();
        doReturn(proxyUrlTls).when(proxyService).getServiceUrlTls();
        doReturn(proxyUrl).when(proxyService).getServiceUrl();

        ProxyConnection proxyConnection = new ProxyConnection(proxyService, null);
        ClientConfigurationData clientConfiguration = proxyConnection.createClientConfiguration();
        assertEquals(clientConfiguration.getServiceUrl(), proxyUrlTls);

        proxyConfiguration.setTlsEnabledWithBroker(false);
        clientConfiguration = proxyConnection.createClientConfiguration();
        assertEquals(clientConfiguration.getServiceUrl(), proxyUrl);
    }

    /**
     * PIP-478: the proxy's lookup leg must carry all three broker-client provider pins, not a subset.
     *
     * <p>The {@code ClientConfigurationData} field is the only route: the lookup client's policy is composed
     * by {@code ClientTlsFactorySupport.clientDefaultPolicy}, which reads it. The direct (broker-connection)
     * leg reads {@code brokerClientJcaProvider} through {@code ProxyTlsFactories.brokerClientPolicy} instead,
     * so a key dropped here makes one {@code proxy.conf} setting govern one of its two outbound legs and
     * silently skip the other — leaving the lookup leg parsing key material on the JVM search order while the
     * operator believes both are pinned.
     */
    @Test
    public void theLookupLegCarriesEveryBrokerClientProviderPin() {
        ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
        proxyConfiguration.setTlsEnabledWithBroker(true);
        proxyConfiguration.setBrokerClientSslProvider("JDK");
        proxyConfiguration.setBrokerClientJsseProvider("SunJSSE");
        proxyConfiguration.setBrokerClientJcaProvider("SUN");

        ProxyService proxyService = mock(ProxyService.class);
        doReturn(proxyConfiguration).when(proxyService).getConfiguration();
        doReturn("pulsar+ssl://proxy:6651").when(proxyService).getServiceUrlTls();

        ClientConfigurationData clientConfiguration =
                new ProxyConnection(proxyService, null).createClientConfiguration();

        assertThat(clientConfiguration.getSslProvider()).isEqualTo("JDK");
        assertThat(clientConfiguration.getJsseProvider()).isEqualTo("SunJSSE");
        assertThat(clientConfiguration.getJcaProvider())
                .as("the JCA pin must reach the lookup leg too: BCJSSE without BCFIPS underneath it is "
                        + "FIPS-shaped rather than FIPS-compliant")
                .isEqualTo("SUN");
    }
}
