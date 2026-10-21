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
import java.security.Security;
import java.util.Set;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.util.tls.JcaProviders;
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
    public void hostnameVerificationIsAnOutboundSettingAppliedToTheBrokerClientPolicy() {
        // tlsHostnameVerificationEnabled is the broker's OUTBOUND setting ("whether the hostname is
        // validated when the broker creates a TLS connection with other brokers"), so it must reach the
        // client-role BROKER_CLIENT policy in both positions — and only that one. A server-role policy
        // carrying it would invite endpoint identification on a server engine, i.e. verifying the
        // connecting client's hostname.
        ServiceConfiguration enabled = conf();
        enabled.setTlsHostnameVerificationEnabled(true);
        assertThat(DefaultBrokerTlsFactory.brokerClientPolicy(enabled).enableHostnameVerification()).isTrue();

        ServiceConfiguration disabled = conf();
        disabled.setTlsHostnameVerificationEnabled(false);
        assertThat(DefaultBrokerTlsFactory.brokerClientPolicy(disabled).enableHostnameVerification()).isFalse();

        assertThat(DefaultBrokerTlsFactory.serverPolicy(enabled).enableHostnameVerification()).isFalse();
        assertThat(DefaultBrokerTlsFactory.webPolicy(enabled).enableHostnameVerification()).isFalse();
    }

    @Test
    public void theDefaultConfigurationPinsNoJsseProviderOnTheNonWebPurposes() {
        // A configured JSSE provider is pinned and fails startup when it cannot be resolved, so the binary
        // and broker-client purposes must not pin one out of the box — only the web listener carries a
        // default, and only conditionally (see below).
        ServiceConfiguration conf = conf();

        assertThat(DefaultBrokerTlsFactory.serverPolicy(conf).jsseProvider()).isNull();
        assertThat(DefaultBrokerTlsFactory.brokerClientPolicy(conf).jsseProvider()).isNull();
    }

    @Test
    public void theWebListenerDefaultsToConscryptOnlyWhenItIsAvailable() {
        // Conscrypt was the shipped default for webServiceTlsProvider before PIP-478 and is kept, but the
        // default is now conditional on Conscrypt actually loading: the value is pinned into the built
        // SSLContext rather than reaching Jetty's inert setProvider(...), so an unconditional default would
        // fail startup wherever the native library is missing (the uber jar covers x86_64 and, since 2.6.1,
        // aarch64 — not every platform). Both branches are asserted so this holds either way.
        ServiceConfiguration conf = conf();

        String webProvider = DefaultBrokerTlsFactory.webPolicy(conf).jsseProvider();
        if (JcaProviders.CONSCRYPT_PROVIDER == null) {
            assertThat(webProvider).as("no Conscrypt on this platform => the JVM default applies").isNull();
        } else {
            assertThat(webProvider).as("Conscrypt is available => it is the web-listener default")
                    .isEqualTo(JcaProviders.CONSCRYPT_PROVIDER.getName());
            // Whatever the default resolves to must be resolvable, or startup would fail on it.
            assertThat(Security.getProvider(webProvider)).as("the default must be a registered provider")
                    .isNotNull();
        }
    }

    @Test
    public void anExplicitWebProviderStillWinsOverTheConscryptDefault() {
        ServiceConfiguration conf = conf();
        conf.setWebServiceTlsProvider("SunJSSE");

        assertThat(DefaultBrokerTlsFactory.webPolicy(conf).jsseProvider()).isEqualTo("SunJSSE");
    }

    @Test
    public void anEngineLiteralInTheWebProviderSelectsNoJsseProvider() {
        // The provider key is overloaded across two axes; an engine literal must not land on the JSSE one.
        ServiceConfiguration conf = conf();
        conf.setWebServiceTlsProvider("OPENSSL");

        assertThat(DefaultBrokerTlsFactory.webPolicy(conf).jsseProvider()).isNull();
    }
}
