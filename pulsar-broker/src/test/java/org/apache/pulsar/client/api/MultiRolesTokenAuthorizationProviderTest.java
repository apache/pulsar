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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.MultiRolesTokenAuthorizationProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MultiRolesTokenAuthorizationProviderTest extends MockedPulsarServiceBaseTest {
    @SuppressWarnings("deprecation")

    private static final String PROXY_ROLE = "multi-roles-proxy";

    private final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private final String superUserToken;
    private final String normalUserToken;
    @SuppressWarnings("deprecation")

    public MultiRolesTokenAuthorizationProviderTest() {
        Map<String, Object> claims = new HashMap<>();
        Set<String> roles = new HashSet<>();
        roles.add("user1");
        roles.add("superUser");
        claims.put("roles", roles);
        superUserToken = Jwts.builder()
                .setClaims(claims)
                .signWith(secretKey)
                .compact();

        roles = new HashSet<>();
        roles.add("normalUser");
        roles.add("user2");
        roles.add("user5");
        claims.put("roles", roles);
        normalUserToken = Jwts.builder()
                .setClaims(claims)
                .signWith(secretKey)
                .compact();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();

        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);
        conf.setProxyRoles(Set.of(PROXY_ROLE));

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey",
                "data:;base64," + Base64.getEncoder().encodeToString(secretKey.getEncoded()));
        properties.setProperty("tokenAuthClaim", "roles");
        conf.setProperties(properties);

        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters(superUserToken);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);
        conf.setAuthorizationProvider(MultiRolesTokenAuthorizationProvider.class.getName());

        conf.setClusterName(configClusterName);
        conf.setNumExecutorThreadPoolSize(5);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster(configClusterName,
                ClusterData.builder()
                        .serviceUrl(brokerUrl.toString())
                        .build()
        );
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.authentication(new AuthenticationToken(superUserToken));
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {
        pulsarAdminBuilder.authentication(new AuthenticationToken(superUserToken));
    }

    private PulsarAdmin newPulsarAdmin(String token) throws PulsarClientException {
        return PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .requestTimeout(3, TimeUnit.SECONDS)
                .build();
    }

    private PulsarClient newPulsarClient(String token) throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(new AuthenticationToken(token))
                .operationTimeout(3, TimeUnit.SECONDS)
                .build();
    }

    @Test
    public void testAdminRequestWithSuperUserToken() throws Exception {
        String tenant = "superuser-admin-tenant";
        @Cleanup
        PulsarAdmin admin = newPulsarAdmin(superUserToken);
        admin.tenants().createTenant(tenant, TenantInfo.builder()
                .allowedClusters(Sets.newHashSet(configClusterName)).build());
        String namespace = "superuser-admin-namespace";
        admin.namespaces().createNamespace(tenant + "/" + namespace);
        admin.brokers().getAllDynamicConfigurations();
        admin.tenants().getTenants();
        admin.topics().getList(tenant + "/" + namespace);
    }

    @Test
    public void testProduceAndConsumeWithSuperUserToken() throws Exception {
        String tenant = "superuser-client-tenant";
        @Cleanup
        PulsarAdmin admin = newPulsarAdmin(superUserToken);
        admin.tenants().createTenant(tenant, TenantInfo.builder()
                .allowedClusters(Sets.newHashSet(configClusterName)).build());
        String namespace = "superuser-client-namespace";
        admin.namespaces().createNamespace(tenant + "/" + namespace);
        String topic = tenant + "/" + namespace + "/" + "test-topic";

        @Cleanup
        PulsarClient client = newPulsarClient(superUserToken);
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic).create();
        byte[] body = "hello".getBytes(StandardCharsets.UTF_8);
        producer.send(body);

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();
        Message<byte[]> message = consumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(message.getData(), body);
    }

    @Test
    public void testAdminRequestWithNormalUserToken() throws Exception {
        String tenant = "normaluser-admin-tenant";
        @Cleanup
        PulsarAdmin admin = newPulsarAdmin(normalUserToken);

        assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> admin.tenants().createTenant(tenant, TenantInfo.builder()
                        .allowedClusters(Sets.newHashSet(configClusterName)).build()));
    }

    @Test
    public void testProduceAndConsumeWithNormalUserToken() throws Exception {
        String tenant = "normaluser-client-tenant";
        @Cleanup
        PulsarAdmin admin = newPulsarAdmin(superUserToken);
        admin.tenants().createTenant(tenant, TenantInfo.builder()
                .allowedClusters(Sets.newHashSet(configClusterName)).build());
        String namespace = "normaluser-client-namespace";
        admin.namespaces().createNamespace(tenant + "/" + namespace);
        String topic = tenant + "/" + namespace + "/" + "test-topic";

        @Cleanup
        PulsarClient client = newPulsarClient(normalUserToken);
        assertThrows(PulsarClientException.AuthorizationException.class, () -> {
            @Cleanup
            Producer<byte[]> ignored = client.newProducer().topic(topic).create();
        });

        assertThrows(PulsarClientException.AuthorizationException.class, () -> {
            @Cleanup
            Consumer<byte[]> ignored = client.newConsumer().topic(topic)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("test")
                    .subscribe();
        });
    }

    @Test
    public void testProxiedRequestsCheckOriginalPrincipalWithItsOwnRole() throws Exception {
        final String tenant = "multi-roles-proxied-tenant";
        final String namespace = tenant + "/ns";
        final String topicPath = namespace + "/test-topic";
        final String userRole = "proxied-user";
        final String consumerRole = "proxied-topic-consumer";
        final String tenantAdminRole = "proxied-tenant-admin-role";
        Map<String, Object> claims = new HashMap<>();
        // the first role is the primary role of the token, the second one is an extra role of the proxy
        claims.put("roles", List.of(PROXY_ROLE, "superUser"));
        final String superUserProxyToken = Jwts.builder().setClaims(claims).signWith(secretKey).compact();
        claims.put("roles", List.of(PROXY_ROLE, tenantAdminRole));
        final String tenantAdminProxyToken = Jwts.builder().setClaims(claims).signWith(secretKey).compact();

        @Cleanup
        PulsarAdmin superUserAdmin = newPulsarAdmin(superUserToken);
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(tenantAdminRole))
                .allowedClusters(Sets.newHashSet(configClusterName)).build());
        superUserAdmin.namespaces().createNamespace(namespace);
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, consumerRole, Set.of(AuthAction.consume));
        superUserAdmin.topics().createNonPartitionedTopic("persistent://" + topicPath);
        HttpClient httpClient = HttpClient.newHttpClient();

        List<String> superUserPaths = List.of(
                "/admin/v2/brokers/" + configClusterName,
                "/admin/v2/clusters/" + configClusterName);
        List<String> tenantPaths = List.of(
                "/admin/v2/persistent/" + topicPath + "/permissions",
                "/admin/v2/namespaces/" + tenant,
                "/admin/v2/namespaces/" + namespace + "/permissions",
                "/admin/v2/namespaces/" + namespace + "/retention",
                "/admin/v2/persistent/" + topicPath + "/retention",
                "/admin/v2/persistent/" + topicPath + "/stats");

        // an ordinary original principal doesn't get the extra roles of the proxy token
        for (String path : superUserPaths) {
            assertDenied(getThroughProxy(httpClient, superUserProxyToken, userRole, path), path);
        }
        for (String path : tenantPaths) {
            assertDenied(getThroughProxy(httpClient, superUserProxyToken, userRole, path), path);
            assertDenied(getThroughProxy(httpClient, tenantAdminProxyToken, userRole, path), path);
        }

        // the original principal keeps its own permissions
        for (String path : superUserPaths) {
            assertEquals(getThroughProxy(httpClient, superUserProxyToken, "superUser", path), 200, path);
        }
        for (String path : tenantPaths) {
            assertEquals(getThroughProxy(httpClient, superUserProxyToken, "superUser", path), 200, path);
            assertEquals(getThroughProxy(httpClient, tenantAdminProxyToken, tenantAdminRole, path), 200, path);
            assertEquals(getThroughProxy(httpClient, superUserProxyToken, tenantAdminRole, path), 200, path);
        }
        assertEquals(getThroughProxy(httpClient, superUserProxyToken, consumerRole,
                "/admin/v2/persistent/" + topicPath + "/stats"), 200);
    }

    private static void assertDenied(int status, String path) {
        assertTrue(status == 401 || status == 403, path + " returned " + status);
    }

    private int getThroughProxy(HttpClient httpClient, String proxyToken, String originalPrincipal, String path)
            throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(pulsar.getWebServiceAddress() + path))
                .header("Authorization", "Bearer " + proxyToken)
                .header("X-Original-Principal", originalPrincipal)
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }
}
