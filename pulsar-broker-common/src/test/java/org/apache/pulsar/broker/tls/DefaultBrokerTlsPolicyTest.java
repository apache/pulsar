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
package org.apache.pulsar.broker.tls;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Set;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.tls.TlsPolicy;
import org.testng.annotations.Test;

/**
 * Policy composition from {@link ServiceConfiguration}, covering only the base {@code tls*} /
 * {@code webServiceTls*} keys that exist today (the PIP-478 keys arrive with the migration).
 *
 * <p>The web listener has its own provider/protocol/cipher keys, and the {@link org.apache.pulsar.tls.TlsPurpose#WEB}
 * policy must prefer them over the binary-listener ones, falling back when they are unset.
 */
public class DefaultBrokerTlsPolicyTest {

    private static ServiceConfiguration conf() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setTlsTrustCertsFilePath("/tls/ca.pem");
        conf.setTlsCertificateFilePath("/tls/server.pem");
        conf.setTlsKeyFilePath("/tls/server.key");
        return conf;
    }

    @Test
    public void webPolicyPrefersTheWebServiceKeys() {
        ServiceConfiguration conf = conf();
        conf.setTlsProvider("SunJSSE");
        conf.setTlsProtocols(Set.of("TLSv1.2"));
        conf.setTlsCiphers(Set.of("TLS_RSA_WITH_AES_256_CBC_SHA"));
        conf.setWebServiceTlsProvider("Conscrypt");
        conf.setWebServiceTlsProtocols(Set.of("TLSv1.3"));
        conf.setWebServiceTlsCiphers(Set.of("TLS_AES_256_GCM_SHA384"));

        TlsPolicy web = DefaultBrokerTlsFactory.webPolicy(conf);

        assertThat(web.jsseProvider()).isEqualTo("Conscrypt");
        assertThat(web.protocols()).containsExactly("TLSv1.3");
        assertThat(web.ciphers()).containsExactly("TLS_AES_256_GCM_SHA384");
    }

    @Test
    public void webPolicyFallsBackToTheBinaryListenerKeysWhenTheWebOnesAreUnset() {
        ServiceConfiguration conf = conf();
        conf.setTlsProvider("SunJSSE");
        conf.setTlsProtocols(Set.of("TLSv1.2"));
        conf.setTlsCiphers(Set.of("TLS_RSA_WITH_AES_256_CBC_SHA"));
        conf.setWebServiceTlsProvider("");
        conf.setWebServiceTlsProtocols(Set.of());
        conf.setWebServiceTlsCiphers(Set.of());

        TlsPolicy web = DefaultBrokerTlsFactory.webPolicy(conf);

        assertThat(web.jsseProvider()).isEqualTo("SunJSSE");
        assertThat(web.protocols()).containsExactly("TLSv1.2");
        assertThat(web.ciphers()).containsExactly("TLS_RSA_WITH_AES_256_CBC_SHA");
    }

    @Test
    public void binaryListenerPolicyNeverReadsTheWebServiceKeys() {
        ServiceConfiguration conf = conf();
        conf.setTlsProvider("SunJSSE");
        conf.setTlsProtocols(Set.of("TLSv1.2"));
        conf.setWebServiceTlsProvider("Conscrypt");
        conf.setWebServiceTlsProtocols(Set.of("TLSv1.3"));

        TlsPolicy server = DefaultBrokerTlsFactory.serverPolicy(conf);

        assertThat(server.jsseProvider()).isEqualTo("SunJSSE");
        assertThat(server.protocols()).containsExactly("TLSv1.2");
    }

    @Test
    public void serverPoliciesDoNotCarryTheOutboundHostnameVerificationFlag() {
        // tlsHostnameVerificationEnabled is the broker's OUTBOUND setting; only brokerClientPolicy may
        // carry it. A server-role policy carrying it would invite endpoint identification on a server
        // engine, i.e. verifying the client's hostname.
        ServiceConfiguration conf = conf();
        conf.setTlsHostnameVerificationEnabled(true);

        assertThat(DefaultBrokerTlsFactory.serverPolicy(conf).enableHostnameVerification()).isFalse();
        assertThat(DefaultBrokerTlsFactory.webPolicy(conf).enableHostnameVerification()).isFalse();
    }

    @Test
    public void anEngineLiteralInTheWebProviderSelectsNoJsseProvider() {
        // The provider key is overloaded across two axes; an engine literal must not land on the JSSE one.
        ServiceConfiguration conf = conf();
        conf.setWebServiceTlsProvider("OPENSSL");

        assertThat(DefaultBrokerTlsFactory.webPolicy(conf).jsseProvider()).isNull();
    }
}
