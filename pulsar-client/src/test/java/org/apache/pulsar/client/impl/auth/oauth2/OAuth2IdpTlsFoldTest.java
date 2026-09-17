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

    // PIP-478: provider pinning of the IdP leg. Both axes (jsseProvider / jcaProvider) are carried by the
    // CLIENT_OAUTH2 policy, so a FIPS deployment does not parse the IdP certificate — or load an IdP mTLS
    // client key — outside the validated module while the broker connection is pinned.

    @Test
    public void unsetProvidersLeaveBothAxesNull() {
        AuthenticationOAuth2 auth = trustOnlyPlugin("");

        TlsPolicy policy = auth.idpTlsPolicy().orElseThrow();
        assertThat(policy.jsseProvider()).as("backward compatible: unset means the JVM provider search order")
                .isNull();
        assertThat(policy.jcaProvider()).isNull();
    }

    @Test
    public void explicitProviderParametersFoldIntoBothAxes() {
        AuthenticationOAuth2 auth = trustOnlyPlugin(",\"jsseProvider\":\"BCJSSE\",\"jcaProvider\":\"BCFIPS\"");

        TlsPolicy policy = auth.idpTlsPolicy().orElseThrow();
        assertThat(policy.jsseProvider()).isEqualTo("BCJSSE");
        assertThat(policy.jcaProvider()).isEqualTo("BCFIPS");
        assertThat(policy.trustCertsFilePath()).as("the IdP material is still folded").isEqualTo("/certs/ca.pem");
    }

    @Test
    public void explicitProviderParametersFoldForTlsClientAuthFlow() {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{"
                + "\"type\":\"client_credentials\","
                + "\"tokenEndpointAuthMethod\":\"tls_client_auth\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"tlsCertFile\":\"/certs/idp-client.cert.pem\","
                + "\"tlsKeyFile\":\"/certs/idp-client.key.pem\","
                + "\"jsseProvider\":\"BCJSSE\",\"jcaProvider\":\"BCFIPS\"}");

        TlsPolicy policy = auth.idpTlsPolicy().orElseThrow();
        assertThat(policy.jsseProvider()).isEqualTo("BCJSSE");
        assertThat(policy.jcaProvider()).isEqualTo("BCFIPS");
    }

    @Test
    public void inheritedProvidersApplyWhenNoParameterIsSet() {
        AuthenticationOAuth2 auth = trustOnlyPlugin("");

        TlsPolicy policy = auth.idpTlsPolicy("BCJSSE", "BCFIPS").orElseThrow();
        assertThat(policy.jsseProvider()).isEqualTo("BCJSSE");
        assertThat(policy.jcaProvider()).isEqualTo("BCFIPS");
    }

    @Test
    public void explicitParametersWinOverInheritedProvidersPerAxis() {
        AuthenticationOAuth2 auth = trustOnlyPlugin(",\"jsseProvider\":\"SunJSSE\"");

        TlsPolicy policy = auth.idpTlsPolicy("BCJSSE", "BCFIPS").orElseThrow();
        assertThat(policy.jsseProvider()).as("the explicit OAuth2 parameter wins on its own axis")
                .isEqualTo("SunJSSE");
        assertThat(policy.jcaProvider()).as("the other axis still inherits").isEqualTo("BCFIPS");
    }

    @Test
    public void blankProviderParametersAreTreatedAsUnset() {
        AuthenticationOAuth2 auth = trustOnlyPlugin(",\"jsseProvider\":\"  \",\"jcaProvider\":\"\"");

        assertThat(auth.idpTlsPolicy().orElseThrow().jsseProvider()).isNull();
        assertThat(auth.idpTlsPolicy("BCJSSE", "BCFIPS").orElseThrow().jsseProvider())
                .as("a blank parameter does not shadow the inherited value").isEqualTo("BCJSSE");
    }

    @Test
    public void providersDoNotConjureAPolicyWithoutIdpTlsMaterial() {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{"
                + "\"type\":\"client_credentials\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"privateKey\":\"data:application/json;base64,e30=\","
                + "\"jsseProvider\":\"BCJSSE\",\"jcaProvider\":\"BCFIPS\"}");

        // Unchanged from today: with no IdP TLS material there is no policy to fold, so CLIENT_OAUTH2 keeps
        // resolving to the system default. PIP-478 defers a provider-only policy for that case.
        assertThat(auth.idpTlsPolicy()).isEmpty();
        assertThat(auth.idpTlsPolicy("BCJSSE", "BCFIPS")).isEmpty();
    }

    /**
     * A client-credentials plugin carrying only IdP trust material, plus the given extra JSON parameter
     * fragment (must start with a comma, or be empty).
     */
    private static AuthenticationOAuth2 trustOnlyPlugin(String extraParamsJsonFragment) {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{"
                + "\"type\":\"client_credentials\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"privateKey\":\"data:application/json;base64,e30=\","
                + "\"trustCertsFilePath\":\"/certs/ca.pem\""
                + extraParamsJsonFragment
                + "}");
        return auth;
    }
}
