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
import java.util.concurrent.ThreadFactory;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * PIP-478 (S4-2): a plaintext {@code pulsar://} broker with an OAuth2 plugin whose HTTPS IdP carries its own
 * TLS material must still compose the client TLS factory, so the framework HTTP client resolves the folded
 * {@code CLIENT_OAUTH2} trust rather than the platform default (v4 ran an independent OAuth2 client that
 * honoured IdP TLS regardless of broker TLS). The broker connection itself stays plaintext — binary-transport
 * TLS is gated on {@code useTls} separately. Without IdP TLS material a plaintext client leaves the factory
 * null (the normal path).
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

    private PulsarClientImpl plaintextClientWith(AuthenticationOAuth2 auth) throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setAuthentication(auth);
        ThreadFactory threadFactory =
                new DefaultThreadFactory("oauth2-idp-tls-test", Thread.currentThread().isDaemon());
        eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);
        return new PulsarClientImpl(conf, eventLoopGroup);
    }

    @Test
    public void plaintextBrokerWithIdpTrustComposesTlsFactory() throws Exception {
        AuthenticationOAuth2 auth = offlineOAuth2("{"
                + "\"type\":\"client_credentials\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"privateKey\":\"data:application/json;base64,e30=\","
                + "\"trustCertsFilePath\":\"/certs/ca.pem\"}");
        PulsarClientImpl client = plaintextClientWith(auth);
        try {
            ClientConfigurationData conf = client.getConfiguration();
            assertThat(conf.isUseTls()).isFalse();
            // The IdP trust material forced the factory to be composed even though the broker link is plaintext.
            assertThat(conf.getTlsFactory()).isNotNull();
        } finally {
            client.close();
        }
    }

    @Test
    public void plaintextBrokerWithoutIdpTrustLeavesFactoryNull() throws Exception {
        AuthenticationOAuth2 auth = offlineOAuth2("{"
                + "\"type\":\"client_credentials\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"privateKey\":\"data:application/json;base64,e30=\"}");
        PulsarClientImpl client = plaintextClientWith(auth);
        try {
            ClientConfigurationData conf = client.getConfiguration();
            assertThat(conf.isUseTls()).isFalse();
            // No IdP TLS material and no broker TLS: the factory stays null (CLIENT_OAUTH2 uses system default).
            assertThat(conf.getTlsFactory()).isNull();
        } finally {
            client.close();
        }
    }
}
