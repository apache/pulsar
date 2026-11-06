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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JwtSettingsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void tlsAndJwtAreIndependent() throws Exception {
        ClusterSettings neither = cluster("");
        assertThat(neither.tls()).isEqualTo(TlsSettings.DISABLED);
        assertThat(neither.jwt()).isEqualTo(JwtSettings.DISABLED);
        assertThat(neither.jwt().withBrokerJwtSettings(Map.of("a", "b"))).isEqualTo(Map.of("a", "b"));
        assertThat(neither.jwt().workloadTokenFilePath()).isNull();

        // Authentication over the plain port
        ClusterSettings jwtOnly = cluster("jwt: {enabled: true}\n");
        assertThat(jwtOnly.tls()).isEqualTo(TlsSettings.DISABLED);
        assertThat(jwtOnly.jwt()).isEqualTo(new JwtSettings(true, JwtSettings.RS256));
        assertThat(jwtOnly.serviceUrl("iot-1")).isEqualTo("pulsar://iot-1-pulsar-broker-0:6650");

        ClusterSettings tlsOnly = cluster("tls: {enabled: true}\n");
        assertThat(tlsOnly.jwt()).isEqualTo(JwtSettings.DISABLED);
        assertThat(tlsOnly.serviceUrl("iot-1")).isEqualTo("pulsar+ssl://iot-1-pulsar-broker-0:6651");

        ClusterSettings both = cluster("tls: {enabled: true}\njwt: {enabled: true, signatureAlgorithm: HS256}\n");
        assertThat(both.tls().enabled()).isTrue();
        assertThat(both.jwt()).isEqualTo(new JwtSettings(true, JwtSettings.HS256));
        assertThat(both.jwt().withBrokerJwtSettings(both.tls().withBrokerTlsSettings(Map.of())))
                .containsKeys("brokerServicePortTls", "tlsCertificateFilePath", "authenticationEnabled",
                        "tokenSecretKey");
    }

    @Test
    public void configuresTheBrokersTokenAuthentication() {
        assertThat(new JwtSettings(true, JwtSettings.RS256).withBrokerJwtSettings(Map.of("superUserRoles", "root")))
                .containsExactly(
                        Map.entry("authenticationEnabled", "true"),
                        Map.entry("authenticationProviders",
                                "org.apache.pulsar.broker.authentication.AuthenticationProviderToken"),
                        Map.entry("tokenPublicKey", "file:///pulsar/performance-jwt/public.key"),
                        Map.entry("superUserRoles", "root"),
                        Map.entry("brokerClientAuthenticationPlugin",
                                "org.apache.pulsar.client.impl.auth.AuthenticationToken"),
                        Map.entry("brokerClientAuthenticationParameters",
                                "file:///pulsar/performance-jwt/admin.token"));
        assertThat(new JwtSettings(true, JwtSettings.HS256).withBrokerJwtSettings(Map.of()))
                .containsEntry("tokenSecretKey", "file:///pulsar/performance-jwt/secret.key")
                .doesNotContainKey("tokenPublicKey")
                .containsEntry("superUserRoles", "admin");
        assertThat(new JwtSettings(true, JwtSettings.RS256).workloadTokenFilePath())
                .isEqualTo("/pulsar/performance-jwt/iot-workload.token");
    }

    @Test
    public void readsTheSettings() throws Exception {
        assertThat(JwtSettings.read(mapper.readTree("{enabled: false}")).enabled()).isFalse();
        // As an environment override or --set gives it
        assertThat(JwtSettings.read(mapper.readTree("{enabled: \"true\"}")).enabled()).isTrue();

        assertThatThrownBy(() -> JwtSettings.read(mapper.readTree("true")))
                .hasMessageContaining("cluster.jwt must be a mapping");
        assertThatThrownBy(() -> JwtSettings.read(mapper.readTree("{enabled: maybe}")))
                .hasMessageContaining("cluster.jwt.enabled must be true or false");
        assertThatThrownBy(() -> JwtSettings.read(mapper.readTree("{enabled: true, signatureAlgorithm: ES256}")))
                .hasMessageContaining("cluster.jwt.signatureAlgorithm must be one of [RS256, HS256]");
        assertThatThrownBy(() -> JwtSettings.read(mapper.readTree("{enabled: true, authorization: true}")))
                .hasMessageContaining("cluster.jwt.authorization isn't a setting");
    }

    @DataProvider
    public Object[][] signatureAlgorithms() {
        return new Object[][] {{JwtSettings.RS256}, {JwtSettings.HS256}};
    }

    @Test(dataProvider = "signatureAlgorithms")
    public void writesTokensThatTheBrokersTokenProviderAccepts(String signatureAlgorithm) throws Exception {
        Path directory = Files.createTempDirectory("jwt-settings-test").resolve("jwt");
        JwtSettings jwt = new JwtSettings(true, signatureAlgorithm);

        String adminToken = jwt.writeKeysAndTokens(directory);

        assertThat(Files.readString(directory.resolve("admin.token"))).isEqualTo(adminToken);
        // The broker's provider, with the brokers' settings, and the mount in place of the directory
        ServiceConfiguration config = new ServiceConfiguration();
        jwt.withBrokerJwtSettings(Map.of()).forEach((name, value) -> config.getProperties().setProperty(name,
                value.replace(JwtSettings.MOUNT, directory.toString())));
        try (AuthenticationProviderToken provider = new AuthenticationProviderToken()) {
            provider.initialize(AuthenticationProvider.Context.builder().config(config).build());

            assertThat(provider.authenticate(new AuthenticationDataCommand(adminToken))).isEqualTo("admin");
            assertThat(provider.authenticate(new AuthenticationDataCommand(
                    Files.readString(directory.resolve("iot-workload.token"))))).isEqualTo("iot-workload");
            // A token that another run's keys signed
            String otherRunsToken = jwt.writeKeysAndTokens(Files.createTempDirectory("jwt-settings-test"));
            assertThatThrownBy(() -> provider.authenticate(new AuthenticationDataCommand(otherRunsToken)))
                    .isInstanceOf(AuthenticationException.class);
        }
    }

    private ClusterSettings cluster(String sections) throws Exception {
        return ClusterSettings.read(mapper, mapper.readTree("brokers: {replicas: 1}\nbookies: {replicas: 1}\n"
                + sections));
    }
}
