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
package org.apache.pulsar.client.impl.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import org.apache.pulsar.tls.TlsPolicy;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 4a: OAuth2 plugin-carried IdP TLS parameters fold into a {@code CLIENT_OAUTH2}
 * {@link TlsPolicy} (issue #24944). Verifies {@link AuthenticationOAuth2#idpTlsPolicy()} composes the
 * policy from the flow's {@code trustCertsFilePath} / {@code tlsCertFile} / {@code tlsKeyFile} parameters
 * (pure configuration parsing; no network).
 */
public class OAuth2IdpTlsFoldTest {

    @Test
    public void clientCredentialsTrustOnlyFoldsIntoClientOauth2Policy() {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{"
                + "\"type\":\"client_credentials\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"privateKey\":\"data:application/json;base64,e30=\","
                + "\"trustCertsFilePath\":\"/certs/ca.pem\"}");

        Optional<TlsPolicy> policy = auth.idpTlsPolicy();
        assertThat(policy).isPresent();
        assertThat(policy.get().format()).isEqualTo(TlsPolicy.Format.PEM);
        assertThat(policy.get().trustCertsFilePath()).isEqualTo("/certs/ca.pem");
        assertThat(policy.get().certificateFilePath()).isNull();
        assertThat(policy.get().keyFilePath()).isNull();
    }

    @Test
    public void tlsClientAuthMtlsFoldsTrustCertAndKey() {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{"
                + "\"type\":\"client_credentials\","
                + "\"tokenEndpointAuthMethod\":\"tls_client_auth\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"tlsCertFile\":\"/certs/idp-client.cert.pem\","
                + "\"tlsKeyFile\":\"/certs/idp-client.key.pem\","
                + "\"trustCertsFilePath\":\"/certs/ca.pem\"}");

        Optional<TlsPolicy> policy = auth.idpTlsPolicy();
        assertThat(policy).isPresent();
        assertThat(policy.get().trustCertsFilePath()).isEqualTo("/certs/ca.pem");
        assertThat(policy.get().certificateFilePath()).isEqualTo("/certs/idp-client.cert.pem");
        assertThat(policy.get().keyFilePath()).isEqualTo("/certs/idp-client.key.pem");
    }

    @Test
    public void noIdpTlsMaterialFoldsToEmpty() {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{"
                + "\"type\":\"client_credentials\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"privateKey\":\"data:application/json;base64,e30=\"}");

        // No trust/cert/key parameters — the IdP call uses the system default trust store (CLIENT_OAUTH2's
        // empty fallback), so there is nothing to fold.
        assertThat(auth.idpTlsPolicy()).isEmpty();
    }

    @Test
    public void unconfiguredPluginFoldsToEmpty() {
        assertThat(new AuthenticationOAuth2().idpTlsPolicy()).isEmpty();
    }
}
