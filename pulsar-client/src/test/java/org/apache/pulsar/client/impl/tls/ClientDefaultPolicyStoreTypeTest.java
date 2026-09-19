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
package org.apache.pulsar.client.impl.tls;

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.tls.TlsPolicy;
import org.testng.annotations.Test;

/**
 * PIP-478 (v4 parity): {@code clientDefaultPolicy} maps the keystore and truststore types independently.
 * The pre-fix code collapsed them into a single {@code storeType} via a {@code keyStorePath != null} heuristic,
 * silently dropping the truststore type in a mixed setup (e.g. a PKCS12 keystore with a JKS truststore).
 */
public class ClientDefaultPolicyStoreTypeTest {

    @Test
    public void mapsSeparateKeyAndTrustStoreTypes() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setUseKeyStoreTls(true);
        conf.setTlsKeyStorePath("/path/key.p12");
        conf.setTlsKeyStorePassword("kpw");
        conf.setTlsKeyStoreType("PKCS12");
        conf.setTlsTrustStorePath("/path/trust.jks");
        conf.setTlsTrustStorePassword("tpw");
        conf.setTlsTrustStoreType("JKS");

        TlsPolicy policy = ClientTlsFactorySupport.clientDefaultPolicy(conf);

        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.KEYSTORE);
        assertThat(policy.keyStoreType()).as("keystore type mapped independently").isEqualTo("PKCS12");
        assertThat(policy.trustStoreType()).as("truststore type NOT clobbered by the keystore type")
                .isEqualTo("JKS");
        assertThat(policy.keyStorePath()).isEqualTo("/path/key.p12");
        assertThat(policy.trustStorePath()).isEqualTo("/path/trust.jks");
    }

    @Test
    public void trustStoreTypePreservedWhenNoKeyStorePath() {
        // Trust-only keystore config (no client identity): the truststore type must still map — the old
        // heuristic fell back to the truststore type only when the keystore path was null, which happened to
        // work here but broke the moment a keystore path was also present.
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setUseKeyStoreTls(true);
        conf.setTlsTrustStorePath("/path/trust.jks");
        conf.setTlsTrustStorePassword("tpw");
        conf.setTlsTrustStoreType("JKS");

        TlsPolicy policy = ClientTlsFactorySupport.clientDefaultPolicy(conf);

        assertThat(policy.trustStoreType()).isEqualTo("JKS");
        assertThat(policy.keyStorePath()).isNull();
    }

    // PIP-478 Part D: an explicit jsseProvider config key maps onto TlsPolicy.jsseProvider.
    @Test
    public void explicitJsseProviderKeyMapsToPolicy() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setJsseProvider("BCJSSE");
        conf.setSslProvider("OPENSSL"); // engine axis; must not clobber the explicit jsseProvider

        TlsPolicy policy = ClientTlsFactorySupport.clientDefaultPolicy(conf);

        assertThat(policy.jsseProvider()).isEqualTo("BCJSSE");
    }

    // PIP-478 Part D: the v4 sslProvider two-axis split — a non-OPENSSL provider name migrates to jsseProvider
    // (restoring v4 behavior), while OPENSSL/OPENSSL_REFCNT stay engine-only and leave jsseProvider unset.
    @Test
    public void sslProviderProviderNameMigratesToJsseProvider() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setSslProvider("Conscrypt");

        assertThat(ClientTlsFactorySupport.clientDefaultPolicy(conf).jsseProvider()).isEqualTo("Conscrypt");
    }

    @Test
    public void openSslLiteralsStayEngineOnlyAndLeaveJsseProviderUnset() {
        for (String engine : new String[] {"OPENSSL", "OPENSSL_REFCNT", "openssl"}) {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setSslProvider(engine);
            assertThat(ClientTlsFactorySupport.clientDefaultPolicy(conf).jsseProvider())
                    .as("engine literal '" + engine + "' does not populate jsseProvider").isNull();
        }
        // Unset sslProvider + unset jsseProvider -> null.
        assertThat(ClientTlsFactorySupport.clientDefaultPolicy(new ClientConfigurationData()).jsseProvider()).isNull();
    }

    // PIP-478 (FIX): sslProvider=JDK is a valid Netty SslProvider engine literal (v4 accepted it) — it must
    // stay on the ENGINE axis and leave jsseProvider unset, NOT be misrouted to a (non-existent) JSSE provider
    // named "JDK", which JcaProviders.resolveNamedProvider would reject loudly at build. Regression guard.
    @Test
    public void jdkEngineLiteralStaysEngineOnlyWhileOtherProviderMigrates() {
        for (String jdk : new String[] {"JDK", "jdk", " Jdk "}) {
            ClientConfigurationData conf = new ClientConfigurationData();
            conf.setSslProvider(jdk);
            assertThat(ClientTlsFactorySupport.clientDefaultPolicy(conf).jsseProvider())
                    .as("engine literal '" + jdk + "' leaves jsseProvider unset").isNull();
        }
        // A non-engine-literal value (a JSSE provider name) still migrates to jsseProvider (v4 parity).
        ClientConfigurationData bcjsse = new ClientConfigurationData();
        bcjsse.setSslProvider("BCJSSE");
        assertThat(ClientTlsFactorySupport.clientDefaultPolicy(bcjsse).jsseProvider()).isEqualTo("BCJSSE");
    }
}
