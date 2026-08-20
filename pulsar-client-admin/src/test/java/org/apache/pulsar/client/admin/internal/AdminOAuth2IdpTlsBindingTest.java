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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.Test;

/**
 * PIP-478 / issue #24944: a {@code PulsarAdmin} using OAuth2 with its own IdP TLS material
 * ({@code trustCertsFilePath} / {@code tlsCertFile} / {@code tlsKeyFile}) must keep honouring it.
 *
 * <p>The remedy mirrors {@code PulsarClientImpl}: fold the IdP policy into the admin's own TLS factory under
 * {@link TlsPurpose#CLIENT_OAUTH2}, rather than leaving the plugin to self-provision a standalone factory.
 * Folding preserves the admin's transport settings — notably its SOCKS5 proxy scope, which a fresh
 * {@code ClientConfigurationData} would have reset to {@code BINARY_ONLY} — and avoids a duplicate factory
 * and refresh scheduler per admin.
 */
public class AdminOAuth2IdpTlsBindingTest {

    private static final String ISSUER = "{\"type\":\"client_credentials\","
            + "\"issuerUrl\":\"https://idp.example.com\","
            + "\"privateKey\":\"data:application/json;base64,e30=\","
            + "\"audience\":\"test\"";

    @Test
    public void idpTrustMaterialIsFoldedIntoTheAdminFactory() throws Exception {
        try (PulsarAdminImpl admin = adminWith(oauth2(ISSUER + ",\"trustCertsFilePath\":\"/tls/idp-ca.pem\"}"))) {
            TlsPolicy idp = admin.getClientConfigData().getTlsPolicyMap().get(TlsPurpose.CLIENT_OAUTH2);
            assertThat(idp).as("the IdP policy must reach the admin's own factory").isNotNull();
            assertThat(idp.trustCertsFilePath()).isEqualTo("/tls/idp-ca.pem");
        }
    }

    @Test
    public void idpMtlsMaterialIsFoldedToo() throws Exception {
        try (PulsarAdminImpl admin = adminWith(oauth2(ISSUER
                + ",\"tlsCertFile\":\"/tls/idp-client.pem\",\"tlsKeyFile\":\"/tls/idp-client.key\"}"))) {
            TlsPolicy idp = admin.getClientConfigData().getTlsPolicyMap().get(TlsPurpose.CLIENT_OAUTH2);
            assertThat(idp).isNotNull();
            assertThat(idp.certificateFilePath()).isEqualTo("/tls/idp-client.pem");
            assertThat(idp.keyFilePath()).isEqualTo("/tls/idp-client.key");
        }
    }

    @Test
    public void anExplicitOauth2PolicyWins() throws Exception {
        // The fold is putIfAbsent, so a policy the caller supplied explicitly must not be overwritten by the
        // one derived from authParams.
        TlsPolicy explicit = TlsPolicy.builder().trustCertsFilePath("/tls/explicit-ca.pem").build();
        PulsarAdminBuilder builder = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(oauth2(ISSUER + ",\"trustCertsFilePath\":\"/tls/idp-ca.pem\"}"));
        ((PulsarAdminBuilderImpl) builder).getConf()
                .setTlsPolicyMap(new java.util.LinkedHashMap<>(java.util.Map.of(TlsPurpose.CLIENT_OAUTH2, explicit)));
        try (PulsarAdminImpl admin = (PulsarAdminImpl) builder.build()) {
            assertThat(admin.getClientConfigData().getTlsPolicyMap().get(TlsPurpose.CLIENT_OAUTH2)
                    .trustCertsFilePath()).isEqualTo("/tls/explicit-ca.pem");
        }
    }

    /**
     * PIP-478: the IdP leg inherits the admin's own provider pins. The admin folds this policy itself, before
     * {@code ClientTlsFactorySupport.composePolicies} runs and into the same map, so that later
     * {@code putIfAbsent} can never supply the inheritance — it has to happen at the admin's fold site or not
     * at all. Without it a FIPS admin parses the IdP certificate outside the module it pinned.
     *
     * <p>Resolvable provider names are used rather than BCJSSE/BCFIPS placeholders: a pinned name is
     * resolved when the factory is built, and an unresolvable one fails the admin build by design.
     */
    @Test
    public void theIdpLegInheritsTheAdminsProviderPins() throws Exception {
        PulsarAdminBuilder builder = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(oauth2(ISSUER + ",\"trustCertsFilePath\":\"/tls/idp-ca.pem\"}"));
        ((PulsarAdminBuilderImpl) builder).getConf().setJsseProvider("SunJSSE");
        ((PulsarAdminBuilderImpl) builder).getConf().setJcaProvider("SUN");

        try (PulsarAdminImpl admin = (PulsarAdminImpl) builder.build()) {
            TlsPolicy idp = admin.getClientConfigData().getTlsPolicyMap().get(TlsPurpose.CLIENT_OAUTH2);
            assertThat(idp.jsseProvider()).isEqualTo("SunJSSE");
            assertThat(idp.jcaProvider()).isEqualTo("SUN");
        }
    }

    @Test
    public void anExplicitOauth2ParameterStillWinsOverTheAdminsPin() throws Exception {
        PulsarAdminBuilder builder = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(oauth2(ISSUER
                        + ",\"trustCertsFilePath\":\"/tls/idp-ca.pem\",\"jcaProvider\":\"SunRsaSign\"}"));
        ((PulsarAdminBuilderImpl) builder).getConf().setJsseProvider("SunJSSE");
        ((PulsarAdminBuilderImpl) builder).getConf().setJcaProvider("SUN");

        try (PulsarAdminImpl admin = (PulsarAdminImpl) builder.build()) {
            TlsPolicy idp = admin.getClientConfigData().getTlsPolicyMap().get(TlsPurpose.CLIENT_OAUTH2);
            assertThat(idp.jcaProvider()).as("the explicit parameter wins on its own axis")
                    .isEqualTo("SunRsaSign");
            assertThat(idp.jsseProvider()).as("the other axis still inherits").isEqualTo("SunJSSE");
        }
    }

    @Test
    public void noIdpMaterialAddsNoPolicy() throws Exception {
        try (PulsarAdminImpl admin = adminWith(oauth2(ISSUER + "}"))) {
            var map = admin.getClientConfigData().getTlsPolicyMap();
            assertThat(map == null || map.get(TlsPurpose.CLIENT_OAUTH2) == null)
                    .as("nothing to fold, so no CLIENT_OAUTH2 policy is invented").isTrue();
        }
    }

    private static PulsarAdminImpl adminWith(AuthenticationOAuth2 auth) throws Exception {
        return (PulsarAdminImpl) PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(auth)
                .build();
    }

    /** start() is stubbed: it would fetch OAuth2 server metadata; the fold runs before it. */
    private static AuthenticationOAuth2 oauth2(String params) {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2() {
            private static final long serialVersionUID = 1L;

            @Override
            public void start() {
            }
        };
        auth.configure(params);
        return auth;
    }
}
