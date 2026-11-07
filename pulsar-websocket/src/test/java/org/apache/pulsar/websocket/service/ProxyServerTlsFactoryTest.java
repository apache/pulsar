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
package org.apache.pulsar.websocket.service;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Set;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.common.tls.TlsClientAuthMode;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ProxyServerTlsFactoryTest {

    @Test
    @SuppressWarnings("deprecation")
    public void clientAuthModeMapsIntoFactorySettings() {
        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        assertThat(clientAuthMode(config)).as("no TLS authentication").isEqualTo(TlsClientAuthMode.NONE);
        config.setAuthenticationEnabled(true);
        config.setAuthenticationProviders(Set.of(AuthenticationProviderTls.class.getName()));
        assertThat(clientAuthMode(config)).isEqualTo(TlsClientAuthMode.OPTIONAL);
        config.setTlsRequireTrustedClientCertOnConnect(true);
        assertThat(clientAuthMode(config)).isEqualTo(TlsClientAuthMode.REQUIRE);
        config.setTlsRequireTrustedClientCertOnConnect(false);
        config.setTlsClientAuthMode(TlsClientAuthMode.NONE);
        assertThat(clientAuthMode(config)).isEqualTo(TlsClientAuthMode.NONE);
    }

    @Test
    public void clientAuthTrustReplacesWebListenerTrust() {
        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setTlsTrustCertsFilePath("/bundle.pem");
        assertThat(ProxyServer.webPolicy(config).trustCertsFilePath()).isEqualTo("/bundle.pem");
        config.setTlsClientAuthTrustCertsFilePath("/client-ca.pem");
        assertThat(ProxyServer.webPolicy(config).trustCertsFilePath()).isEqualTo("/client-ca.pem");

        config.setTlsEnabledWithKeyStore(true);
        config.setTlsTrustStore("/trust.jks");
        assertThat(ProxyServer.webPolicy(config).trustStorePath()).isEqualTo("/trust.jks");
        config.setTlsClientAuthTrustStore("/client-ca.p12");
        assertThat(ProxyServer.webPolicy(config).trustStorePath()).isEqualTo("/client-ca.p12");
    }

    private static TlsClientAuthMode clientAuthMode(WebSocketProxyConfiguration config) {
        return ((FileBasedTlsFactory) ProxyServer.buildDefaultWebTlsFactory(config)).settings().clientAuthMode();
    }
}
