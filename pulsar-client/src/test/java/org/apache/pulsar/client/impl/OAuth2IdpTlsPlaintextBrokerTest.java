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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * PIP-478 (S4-2): a plaintext {@code pulsar://} broker whose configuration nonetheless describes a
 * {@code CLIENT_OAUTH2} trust domain must still compose the client TLS factory, so the framework HTTP client
 * resolves that trust rather than the platform default (v4 ran an independent OAuth2 client that honoured IdP
 * TLS regardless of broker TLS). The broker connection itself stays plaintext — binary-transport TLS is gated
 * on {@code useTls} separately. Without any IdP trust configured a plaintext client leaves the factory null
 * (the normal path), which is what makes the system default correct there.
 *
 * <p>The trust domain can be described through more than one entry point, and the same silent-downgrade
 * defect was found on three different paths, so the entry points are asserted together rather than one test
 * per path. Composing the factory is the whole of the fix: a null factory is exactly the downgrade, because
 * the framework HTTP client then builds its engines from platform-default trust.
 */
public class OAuth2IdpTlsPlaintextBrokerTest {

    private EventLoopGroup eventLoopGroup;

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully().get();
            eventLoopGroup = null;
        }
    }

    /**
     * An {@link AuthenticationOAuth2} whose {@code start()} skips the eager IdP metadata fetch (a network
     * call), so a unit test can construct the client offline. {@code idpTlsPolicy()} is resolved from the
     * flow created during {@code configure()}, independent of {@code start()}.
     */
    private static AuthenticationOAuth2 offlineOAuth2(String config) {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2() {
            @Override
            public void start() {
                // Skip the eager IdP metadata fetch; the fold under test happens at TLS-factory compose time.
            }
        };
        auth.configure(config);
        return auth;
    }

    /** The OAuth2 plugin config used throughout, with {@code trustCertsFilePath} appended when given. */
    private static String oauth2Config(String idpTrustCertsFilePath) {
        return "{"
                + "\"type\":\"client_credentials\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"privateKey\":\"data:application/json;base64,e30=\""
                + (idpTrustCertsFilePath == null ? "" : ",\"trustCertsFilePath\":\"" + idpTrustCertsFilePath + "\"")
                + "}";
    }

    private static ClientConfigurationData plaintextConf() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        return conf;
    }

    private PulsarClientImpl clientFor(ClientConfigurationData conf) throws Exception {
        ThreadFactory threadFactory =
                new DefaultThreadFactory("oauth2-idp-tls-test", Thread.currentThread().isDaemon());
        eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);
        return new PulsarClientImpl(conf, eventLoopGroup);
    }

    /**
     * The ways a caller can describe a {@code CLIENT_OAUTH2} trust domain while leaving the broker transport
     * plaintext. The material is never loaded here (the default file-based factory loads lazily), so the paths
     * need not exist — what is under test is whether the factory is composed at all.
     *
     * @return {entry-point name, configuration to apply to a plaintext client config}
     */
    @DataProvider(name = "idpTrustEntryPoints")
    public static Object[][] idpTrustEntryPoints() {
        TlsPolicy idpPolicy = TlsPolicy.builder().trustCertsFilePath("/certs/idp-ca.pem").build();
        return new Object[][]{
                {"an OAuth2 plugin carrying its own IdP TLS material",
                        (Consumer<ClientConfigurationData>) conf ->
                                conf.setAuthentication(offlineOAuth2(oauth2Config("/certs/ca.pem")))},
                {"an explicit v5 tlsPolicy(CLIENT_OAUTH2, ...)",
                        (Consumer<ClientConfigurationData>) conf -> conf.setTlsPolicyMap(
                                new LinkedHashMap<>(Map.of(TlsPurpose.CLIENT_OAUTH2, idpPolicy)))},
                {"an explicit v5 policy alongside a plugin that carries its own material",
                        (Consumer<ClientConfigurationData>) conf -> {
                            conf.setAuthentication(offlineOAuth2(oauth2Config("/certs/ca.pem")));
                            conf.setTlsPolicyMap(new LinkedHashMap<>(Map.of(TlsPurpose.CLIENT_OAUTH2, idpPolicy)));
                        }},
        };
    }

    @Test(dataProvider = "idpTrustEntryPoints")
    public void configuredIdpTrustIsNeverDowngradedToSystemDefault(String entryPoint,
            Consumer<ClientConfigurationData> configure) throws Exception {
        ClientConfigurationData conf = plaintextConf();
        configure.accept(conf);
        PulsarClientImpl client = clientFor(conf);
        try {
            assertThat(client.getConfiguration().isUseTls())
                    .as("%s: the broker transport must stay plaintext", entryPoint).isFalse();
            assertThat(client.getConfiguration().getTlsFactory())
                    .as("%s: the configured CLIENT_OAUTH2 trust must compose the client TLS factory; leaving it "
                            + "null sends the framework HTTP client to platform-default trust instead", entryPoint)
                    .isNotNull();
        } finally {
            client.close();
        }
    }

    @Test
    public void plaintextBrokerWithoutIdpTrustLeavesFactoryNull() throws Exception {
        ClientConfigurationData conf = plaintextConf();
        conf.setAuthentication(offlineOAuth2(oauth2Config(null)));
        PulsarClientImpl client = clientFor(conf);
        try {
            assertThat(client.getConfiguration().isUseTls()).isFalse();
            // No IdP TLS material and no broker TLS: the factory stays null (CLIENT_OAUTH2 uses system default).
            assertThat(client.getConfiguration().getTlsFactory()).isNull();
        } finally {
            client.close();
        }
    }
}
