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
package org.apache.pulsar.client.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

/**
 * PIP-478 / issue #24944: a {@code PulsarAdmin} using OAuth2 with its own IdP TLS material
 * ({@code trustCertsFilePath} / {@code tlsCertFile} / {@code tlsKeyFile}) must keep honouring it.
 *
 * <p>The admin path never sets {@code conf.tlsFactory}, so the framework HTTP client factory the admin binds
 * has no TLS material to serve, and its legacy branch applies material only for {@code CLIENT_DEFAULT} —
 * never for the {@code CLIENT_OAUTH2} purpose OAuth2 requests. Binding it is also what makes {@code FlowBase}
 * skip {@code StandaloneOAuth2HttpClientFactory}, the only consumer of {@code idpTlsPolicy()}. So such a
 * plugin has to be left unbound; v4 honoured the material and 5.0 must too.
 */
public class AdminOAuth2IdpTlsBindingTest {

    private static final String ISSUER = "{\"type\":\"client_credentials\","
            + "\"issuerUrl\":\"https://idp.example.com\","
            + "\"privateKey\":\"data:application/json;base64,e30=\","
            + "\"audience\":\"test\"";

    @Test
    public void anOAuth2PluginWithIdpTlsMaterialIsLeftStandalone() {
        AuthenticationOAuth2 auth = oauth2(ISSUER + ",\"trustCertsFilePath\":\"/tls/idp-ca.pem\"}");
        assertThat(auth.idpTlsPolicy()).as("precondition: the flow carries IdP TLS material").isPresent();

        assertThat(PulsarAdminImpl.leaveOAuth2Standalone(auth, new ClientConfigurationData()))
                .as("binding the framework factory would silently drop the IdP material")
                .isTrue();
    }

    @Test
    public void mtlsMaterialAlsoLeavesThePluginStandalone() {
        AuthenticationOAuth2 auth = oauth2(ISSUER
                + ",\"tlsCertFile\":\"/tls/idp-client.pem\",\"tlsKeyFile\":\"/tls/idp-client.key\"}");
        assertThat(PulsarAdminImpl.leaveOAuth2Standalone(auth, new ClientConfigurationData())).isTrue();
    }

    @Test
    public void anOAuth2PluginWithoutIdpTlsMaterialIsStillBound() {
        AuthenticationOAuth2 auth = oauth2(ISSUER + "}");
        assertThat(auth.idpTlsPolicy()).as("precondition: no IdP TLS material").isEmpty();

        assertThat(PulsarAdminImpl.leaveOAuth2Standalone(auth, new ClientConfigurationData()))
                .as("with nothing to lose, the framework factory is bound as before")
                .isFalse();
    }

    @Test
    public void aNonOAuth2PluginIsAlwaysBound() {
        assertThat(PulsarAdminImpl.leaveOAuth2Standalone(new AuthenticationToken("t"),
                new ClientConfigurationData())).isFalse();
    }

    /**
     * {@code configure(...)} is what builds the flow that {@code idpTlsPolicy()} reads; {@code start()} is
     * deliberately not called, because it eagerly loads the configured cert files and the binding decision
     * under test does not depend on it.
     */
    private static AuthenticationOAuth2 oauth2(String params) {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(params);
        return auth;
    }
}
