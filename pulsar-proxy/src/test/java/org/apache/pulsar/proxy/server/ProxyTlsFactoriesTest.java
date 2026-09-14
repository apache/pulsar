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
import java.util.Set;
import org.testng.annotations.Test;

public class ProxyTlsFactoriesTest {

    @Test
    public void routesNonEngineTlsProviderToServerPolicyJsseProvider() {
        // A JSSE (SSLContext) provider name set on the engine-axis field must be routed to the policy
        // jsseProvider so a v4 tlsProvider=Conscrypt keystore setup keeps building the SSLContext with that
        // provider (the two-axis split mirrored from the broker / client sslProvider handling).
        ProxyConfiguration config = new ProxyConfiguration();
        config.setTlsProvider("SunJSSE");
        assertThat(ProxyTlsFactories.serverPolicy(config, Set.of(), Set.of()).jsseProvider())
                .isEqualTo("SunJSSE");

        config.setBrokerClientSslProvider("SunJSSE");
        assertThat(ProxyTlsFactories.brokerClientPolicy(config).jsseProvider()).isEqualTo("SunJSSE");
    }

    @Test
    public void keepsEngineLiteralTlsProviderOffJsseProvider() {
        // Engine literals stay on the engine axis and must NOT be misrouted to a (non-existent) JSSE provider.
        for (String engineLiteral : new String[] {"OPENSSL", "OPENSSL_REFCNT", "JDK"}) {
            ProxyConfiguration config = new ProxyConfiguration();
            config.setTlsProvider(engineLiteral);
            config.setBrokerClientSslProvider(engineLiteral);
            assertThat(ProxyTlsFactories.serverPolicy(config, Set.of(), Set.of()).jsseProvider())
                    .as("serverPolicy jsseProvider for tlsProvider=" + engineLiteral).isNull();
            assertThat(ProxyTlsFactories.brokerClientPolicy(config).jsseProvider())
                    .as("brokerClientPolicy jsseProvider for brokerClientSslProvider=" + engineLiteral).isNull();
        }
    }

    @Test
    public void explicitJsseProviderWinsOverEngineAxis() {
        // An explicit jsseProvider takes precedence over the engine-axis field.
        ProxyConfiguration config = new ProxyConfiguration();
        config.setJsseProvider("SunJSSE");
        config.setTlsProvider("OPENSSL");
        assertThat(ProxyTlsFactories.serverPolicy(config, Set.of(), Set.of()).jsseProvider())
                .isEqualTo("SunJSSE");

        config.setBrokerClientJsseProvider("SunJSSE");
        config.setBrokerClientSslProvider("OPENSSL");
        assertThat(ProxyTlsFactories.brokerClientPolicy(config).jsseProvider()).isEqualTo("SunJSSE");
    }
}
