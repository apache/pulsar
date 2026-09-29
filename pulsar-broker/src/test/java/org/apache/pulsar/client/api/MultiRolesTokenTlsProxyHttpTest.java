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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.MultiRolesTokenAuthorizationProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.tls.JdkSslContexts;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Admin requests from a proxy that authenticates to the broker with a TLS client certificate and forwards
 * the client's headers, as the Pulsar proxy does.
 */
@Test(groups = "broker-api")
public class MultiRolesTokenTlsProxyHttpTest extends MockedPulsarServiceBaseTest {

    // CNs of the test client certificates
    private static final String SUPER_USER = "admin";
    private static final String SUPER_USER_PROXY = "superproxy";
    private static final String TENANT_ADMIN_PROXY = "proxy";

    private static final String TENANT = "tls-proxy-tenant";
    private static final String NAMESPACE = TENANT + "/ns";
    private static final String TOPIC = NAMESPACE + "/topic";
    private static final String CLIENT_ROLE = "tls-proxy-client";
    private static final String OTHER_CLIENT_ROLE = "tls-proxy-other-client";
    private static final String CONSUMER_ROLE = "tls-proxy-consumer";
    private static final String TENANT_ADMIN_ROLE = "tls-proxy-tenant-admin";

    @SuppressWarnings("deprecation")
    private final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);

        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        // sorted like the default configuration, so TLS is tried first when no method name is sent
        conf.setAuthenticationProviders(new TreeSet<>(Set.of(AuthenticationProviderTls.class.getName(),
                AuthenticationProviderToken.class.getName())));
        conf.setAuthorizationProvider(MultiRolesTokenAuthorizationProvider.class.getName());
        conf.setSuperUserRoles(Set.of(SUPER_USER, SUPER_USER_PROXY));
        conf.setProxyRoles(Set.of(SUPER_USER_PROXY, TENANT_ADMIN_PROXY));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey",
                "data:;base64," + Base64.getEncoder().encodeToString(secretKey.getEncoded()));
        properties.setProperty("tokenAuthClaim", "roles");
        conf.setProperties(properties);

        conf.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        conf.setBrokerClientAuthenticationParameters(String.format("tlsCertFile:%s,tlsKeyFile:%s",
                getTlsFileForClient(SUPER_USER + ".cert"), getTlsFileForClient(SUPER_USER + ".key-pk8")));
        conf.setBrokerClientTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setBrokerClientTlsEnabled(true);
        conf.setNumExecutorThreadPoolSize(5);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        @Cleanup
        PulsarAdmin superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrlTls.toString())
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .authentication(AuthenticationTls.class.getName(), String.format("tlsCertFile:%s,tlsKeyFile:%s",
                        getTlsFileForClient(SUPER_USER + ".cert"), getTlsFileForClient(SUPER_USER + ".key-pk8")))
                .build();
        superUserAdmin.clusters().createCluster(configClusterName,
                ClusterData.builder().serviceUrl(brokerUrl.toString()).serviceUrlTls(brokerUrlTls.toString()).build());
        superUserAdmin.tenants().createTenant(TENANT, TenantInfo.builder()
                .adminRoles(Set.of(TENANT_ADMIN_PROXY, TENANT_ADMIN_ROLE))
                .allowedClusters(Set.of(configClusterName)).build());
        superUserAdmin.namespaces().createNamespace(NAMESPACE);
        superUserAdmin.namespaces().grantPermissionOnNamespace(NAMESPACE, CONSUMER_ROLE, Set.of(AuthAction.consume));
        superUserAdmin.topics().createNonPartitionedTopic("persistent://" + TOPIC);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testForwardedClientTokenIsUsedForTheOriginalPrincipal() throws Exception {
        String clientToken = createToken(CLIENT_ROLE, CONSUMER_ROLE);
        String otherClientToken = createToken(OTHER_CLIENT_ROLE, CONSUMER_ROLE);
        HttpClient proxyClient = newProxyHttpClient(SUPER_USER_PROXY);
        List<String> paths = List.of(
                "/admin/v2/persistent/" + TOPIC + "/stats",
                "/admin/v2/namespaces/" + NAMESPACE + "/topics");
        for (String path : paths) {
            // the secondary role of the client's own token grants access
            assertEquals(send(proxyClient, path, CLIENT_ROLE, clientToken, null), 200, path);
            // the broker authenticates the forwarded token when the client sent its method name
            assertEquals(send(proxyClient, path, CLIENT_ROLE, clientToken, "token"), 200, path);
            // without the client's own token only the original principal's role is used
            assertDenied(send(proxyClient, path, CLIENT_ROLE, null, null), path);
            assertDenied(send(proxyClient, path, CLIENT_ROLE, otherClientToken, null), path);
            assertDenied(send(proxyClient, path, CLIENT_ROLE, otherClientToken, "token"), path);
        }
    }

    @Test
    public void testTenantAdminProxyWithoutToken() throws Exception {
        HttpClient proxyClient = newProxyHttpClient(TENANT_ADMIN_PROXY);
        // requires tenant admin access for both the proxy and the original principal
        String path = "/admin/v2/namespaces/" + NAMESPACE + "/properties";
        assertEquals(send(proxyClient, path, TENANT_ADMIN_ROLE, null, null), 200);
        assertDenied(send(proxyClient, path, CLIENT_ROLE, null, null), path);
    }

    private String createToken(String... roles) {
        // the first role is the subject
        return Jwts.builder().claim("roles", List.of(roles)).signWith(secretKey).compact();
    }

    private static HttpClient newProxyHttpClient(String proxyCertName) throws Exception {
        return HttpClient.newBuilder()
                .sslContext(JdkSslContexts.createSslContext(false, CA_CERT_FILE_PATH,
                        getTlsFileForClient(proxyCertName + ".cert"),
                        getTlsFileForClient(proxyCertName + ".key-pk8"), null))
                .build();
    }

    /**
     * Sends a request as the proxy: its TLS client certificate, the original principal header and the client's
     * headers as the client sent them.
     */
    private int send(HttpClient proxyClient, String path, String originalPrincipal, String clientToken,
                     String clientAuthMethodName) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(pulsar.getWebServiceAddressTls() + path))
                .header("X-Original-Principal", originalPrincipal)
                .GET();
        if (clientToken != null) {
            request.header("Authorization", "Bearer " + clientToken);
        }
        if (clientAuthMethodName != null) {
            request.header("X-Pulsar-Auth-Method-Name", clientAuthMethodName);
        }
        return proxyClient.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static void assertDenied(int status, String path) {
        assertTrue(status == 401 || status == 403, path + " returned " + status);
    }
}
