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
import java.util.Map;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.Test;

/**
 * PIP-478: on the framework-bound path the OAuth2 IdP leg inherits the client's provider pins.
 *
 * <p>{@code OAuth2IdpTlsFoldTest} pins the precedence rules on the plugin itself, by calling the overload
 * directly. This pins the wiring: that {@code composePolicies} actually hands the composed
 * {@code CLIENT_DEFAULT} provider names to the plugin. Without it a client pinned for FIPS would parse the
 * IdP certificate — and any IdP mTLS client key — through the JVM provider search order while its broker
 * connection is pinned, which is the fail-open half of a FIPS-shaped deployment.
 */
public class OAuth2IdpProviderInheritanceTest {

    @Test
    public void theIdpLegInheritsTheClientsProviderPins() {
        ClientConfigurationData conf = confWithOAuth2(
                "\"trustCertsFilePath\":\"/certs/ca.pem\"");
        conf.setJsseProvider("BCJSSE");
        conf.setJcaProvider("BCFIPS");

        TlsPolicy idp = ClientTlsFactorySupport.composePolicies(conf).get(TlsPurpose.CLIENT_OAUTH2);

        assertThat(idp).as("the plugin carries IdP TLS material, so a CLIENT_OAUTH2 policy is folded")
                .isNotNull();
        assertThat(idp.jsseProvider()).isEqualTo("BCJSSE");
        assertThat(idp.jcaProvider())
                .as("the IdP leg must not parse key material outside the provider the client pinned")
                .isEqualTo("BCFIPS");
    }

    @Test
    public void anExplicitOAuth2ParameterStillWinsOverTheInheritedValue() {
        ClientConfigurationData conf = confWithOAuth2(
                "\"trustCertsFilePath\":\"/certs/ca.pem\",\"jcaProvider\":\"SUN\"");
        conf.setJsseProvider("BCJSSE");
        conf.setJcaProvider("BCFIPS");

        TlsPolicy idp = ClientTlsFactorySupport.composePolicies(conf).get(TlsPurpose.CLIENT_OAUTH2);

        assertThat(idp.jcaProvider()).as("the explicit parameter wins on its own axis").isEqualTo("SUN");
        assertThat(idp.jsseProvider()).as("the other axis still inherits").isEqualTo("BCJSSE");
    }

    @Test
    public void theClientDefaultPolicyKeepsItsOwnPins() {
        ClientConfigurationData conf = confWithOAuth2(
                "\"trustCertsFilePath\":\"/certs/ca.pem\"");
        conf.setJcaProvider("BCFIPS");

        Map<TlsPurpose, TlsPolicy> policies = ClientTlsFactorySupport.composePolicies(conf);

        assertThat(policies.get(TlsPurpose.CLIENT_DEFAULT).jcaProvider())
                .as("folding the IdP policy must not disturb the client's own")
                .isEqualTo("BCFIPS");
    }

    @Test
    public void anUnpinnedClientLeavesTheIdpLegUnpinned() {
        ClientConfigurationData conf = confWithOAuth2(
                "\"trustCertsFilePath\":\"/certs/ca.pem\"");

        TlsPolicy idp = ClientTlsFactorySupport.composePolicies(conf).get(TlsPurpose.CLIENT_OAUTH2);

        assertThat(idp.jsseProvider()).as("unset stays unset: the JVM provider search order").isNull();
        assertThat(idp.jcaProvider()).isNull();
    }

    /**
     * A client configuration whose authentication is an OAuth2 plugin configured with the given extra
     * parameters (a JSON fragment without the enclosing braces).
     */
    private static ClientConfigurationData confWithOAuth2(String extraParamsJson) {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure("{"
                + "\"type\":\"client_credentials\","
                + "\"issuerUrl\":\"https://idp.example.com\","
                + "\"privateKey\":\"data:application/json;base64,e30=\","
                + extraParamsJson
                + "}");
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setAuthentication(auth);
        return conf;
    }
}
