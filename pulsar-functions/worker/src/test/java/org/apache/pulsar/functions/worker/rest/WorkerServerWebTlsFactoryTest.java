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
package org.apache.pulsar.functions.worker.rest;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Set;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.common.tls.TlsClientAuthMode;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.testng.annotations.Test;

public class WorkerServerWebTlsFactoryTest {

    @Test
    @SuppressWarnings("deprecation")
    public void clientAuthModeMapsIntoFactorySettings() {
        WorkerConfig config = new WorkerConfig();
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
        WorkerConfig config = new WorkerConfig();
        config.setTlsTrustCertsFilePath("/bundle.pem");
        assertThat(WorkerServer.webPolicy(config).trustCertsFilePath()).isEqualTo("/bundle.pem");
        config.setTlsClientAuthTrustCertsFilePath("/client-ca.pem");
        assertThat(WorkerServer.webPolicy(config).trustCertsFilePath()).isEqualTo("/client-ca.pem");

        config.setTlsEnabledWithKeyStore(true);
        config.setTlsTrustStore("/trust.jks");
        assertThat(WorkerServer.webPolicy(config).trustStorePath()).isEqualTo("/trust.jks");
        config.setTlsClientAuthTrustStore("/client-ca.p12");
        assertThat(WorkerServer.webPolicy(config).trustStorePath()).isEqualTo("/client-ca.p12");
    }

    private static TlsClientAuthMode clientAuthMode(WorkerConfig config) {
        return ((FileBasedTlsFactory) WorkerServer.buildDefaultWebTlsFactory(config)).settings().clientAuthMode();
    }
}
