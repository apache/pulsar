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
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.awaitility.Awaitility;
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
    public void testNamespaceClearBacklogWithSecondaryTenantAdminRole() throws Exception {
        final String tenant = "multi-roles-clear-backlog-tenant";
        final String namespace = tenant + "/ns";
        final String topic = "persistent://" + namespace + "/test-topic";
        final String subscription = "sub";
        final String bundle = "0x00000000_0xffffffff";
        // resolved to the subscription with the same name as the remote cluster part of the name
        final String replicatorCursor = pulsar.getConfiguration().getReplicatorPrefix() + "." + subscription;
        final int numMessages = 5;
        Map<String, Object> claims = new HashMap<>();
        // the first role is the primary role of the token, the second one is a tenant admin role
        claims.put("roles", List.of("clear-backlog-consumer", "clear-backlog-tenant-admin"));
        final String tenantAdminToken = Jwts.builder().setClaims(claims).signWith(secretKey).compact();
        claims.put("roles", List.of("clear-backlog-consumer"));
        final String consumerToken = Jwts.builder().setClaims(claims).signWith(secretKey).compact();

        @Cleanup
        PulsarAdmin superUserAdmin = newPulsarAdmin(superUserToken);
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of("clear-backlog-tenant-admin"))
                .allowedClusters(Sets.newHashSet(configClusterName)).build());
        superUserAdmin.namespaces().createNamespace(namespace);
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, "clear-backlog-consumer",
                Set.of(AuthAction.consume));
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createSubscription(topic, subscription, MessageId.earliest);
        @Cleanup
        PulsarClient client = newPulsarClient(superUserToken);
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic).enableBatching(false).create();
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }

        @Cleanup
        PulsarAdmin consumerAdmin = newPulsarAdmin(consumerToken);
        assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> consumerAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, replicatorCursor));
        assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> consumerAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle,
                        replicatorCursor));
        assertEquals(superUserAdmin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog(),
                numMessages);

        @Cleanup
        PulsarAdmin tenantAdmin = newPulsarAdmin(tenantAdminToken);
        tenantAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, replicatorCursor);
        assertEquals(superUserAdmin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog(),
                0);
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }
        tenantAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, replicatorCursor);
        assertEquals(superUserAdmin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog(),
                0);
    }

    @Test
    public void testNamespaceSubscriptionOperationsThroughProxyWithTenantAdminRole() throws Exception {
        final String tenant = "multi-roles-proxy-tenant";
        final String namespace = tenant + "/ns";
        final String topic = "persistent://" + namespace + "/test-topic";
        final String consumerRole = "proxied-consumer";
        final String tenantAdminRole = "proxied-tenant-admin";
        final String subscription = "sub";
        final String consumerSubscription = consumerRole + "-sub";
        final String bundle = "0x00000000_0xffffffff";
        // resolved to the subscription with the same name as the remote cluster part of the name
        final String replicatorCursor = pulsar.getConfiguration().getReplicatorPrefix() + "." + subscription;
        final int numMessages = 5;
        Map<String, Object> claims = new HashMap<>();
        // the first role is the primary role of the token, the second one gives the proxy tenant admin access
        claims.put("roles", List.of(PROXY_ROLE, tenantAdminRole));
        final String proxyToken = Jwts.builder().setClaims(claims).signWith(secretKey).compact();

        @Cleanup
        PulsarAdmin superUserAdmin = newPulsarAdmin(superUserToken);
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(tenantAdminRole))
                .allowedClusters(Sets.newHashSet(configClusterName)).build());
        superUserAdmin.namespaces().createNamespace(namespace);
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, consumerRole, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createSubscription(topic, subscription, MessageId.earliest);
        superUserAdmin.topics().createSubscription(topic, consumerSubscription, MessageId.earliest);
        @Cleanup
        PulsarClient client = newPulsarClient(superUserToken);
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic).enableBatching(false).create();
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }
        HttpClient httpClient = HttpClient.newHttpClient();

        // the proxy is a tenant admin through a token role, the original principal is an ordinary consumer and is
        // checked with its own role only
        for (String bundlePath : new String[]{"", "/" + bundle}) {
            assertEquals(clearBacklogThroughProxy(httpClient, proxyToken, consumerRole, namespace + bundlePath,
                    replicatorCursor), 403);
            assertEquals(clearBacklogThroughProxy(httpClient, proxyToken, consumerRole, namespace + bundlePath,
                    subscription), 403);
        }
        assertEquals(superUserAdmin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog(),
                numMessages);

        // the consumer can clear a subscription that matches its role prefix
        assertEquals(clearBacklogThroughProxy(httpClient, proxyToken, consumerRole, namespace,
                consumerSubscription), 204);
        assertEquals(superUserAdmin.topics().getStats(topic).getSubscriptions().get(consumerSubscription)
                .getMsgBacklog(), 0);

        // a tenant admin forwarded by the proxy keeps the existing behaviour
        assertEquals(clearBacklogThroughProxy(httpClient, proxyToken, tenantAdminRole, namespace,
                replicatorCursor), 204);
        assertEquals(superUserAdmin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog(),
                0);
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(clearBacklogThroughProxy(httpClient, proxyToken, tenantAdminRole, namespace + "/" + bundle,
                subscription), 204);
        assertEquals(superUserAdmin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog(),
                0);
    }

    @Test
    public void testAllSubscriptionOperationsThroughProxyWithTenantAdminRole() throws Exception {
        final String tenant = "multi-roles-proxy-all-subscriptions-tenant";
        final String namespace = tenant + "/ns";
        final String topic = "persistent://" + namespace + "/topic";
        final String partitionedTopic = "persistent://" + namespace + "/partitioned";
        final String partitionedTopic2 = "persistent://" + namespace + "/partitioned-2";
        final String consumerRole = "proxied-all-subscriptions-consumer";
        final String tenantAdminRole = "proxied-all-subscriptions-tenant-admin";
        final String subscription = "sub";
        final String consumerSubscription = consumerRole + "-sub";
        final int numMessages = 5;
        Map<String, Object> claims = new HashMap<>();
        // the first role is the primary role of the token, the second one gives the proxy tenant admin access
        claims.put("roles", List.of(PROXY_ROLE, tenantAdminRole));
        final String proxyToken = Jwts.builder().setClaims(claims).signWith(secretKey).compact();

        @Cleanup
        PulsarAdmin superUserAdmin = newPulsarAdmin(superUserToken);
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(tenantAdminRole))
                .allowedClusters(Sets.newHashSet(configClusterName)).build());
        superUserAdmin.namespaces().createNamespace(namespace);
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, consumerRole, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createPartitionedTopic(partitionedTopic, 2);
        superUserAdmin.topics().createPartitionedTopic(partitionedTopic2, 2);
        final List<String> topics = List.of(topic, partitionedTopic + "-partition-0",
                partitionedTopic + "-partition-1", partitionedTopic2 + "-partition-0",
                partitionedTopic2 + "-partition-1");
        @Cleanup
        PulsarClient client = newPulsarClient(superUserToken);
        for (String t : topics) {
            superUserAdmin.topics().createSubscription(t, subscription, MessageId.earliest);
            superUserAdmin.topics().createSubscription(t, consumerSubscription, MessageId.earliest);
            @Cleanup
            Producer<byte[]> producer = client.newProducer().topic(t).enableBatching(false).create();
            for (int i = 0; i < numMessages; i++) {
                producer.send(("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
        // the messages must be older than the expiry time
        Thread.sleep(1500);
        HttpClient httpClient = HttpClient.newHttpClient();

        // the proxy is a tenant admin through a token role, the original principal is an ordinary consumer and
        // expires only the subscriptions that match its role prefix: on a topic, a partitioned topic and a partition
        for (String t : List.of(topic, partitionedTopic, partitionedTopic2 + "-partition-0")) {
            assertEquals(postThroughProxy(httpClient, proxyToken, consumerRole, "/admin/v2/"
                    + TopicName.get(t).getRestPath() + "/all_subscription/expireMessages/1"), 204);
        }
        final List<String> expired = topics.subList(0, 4);
        Awaitility.await().untilAsserted(() -> {
            for (String t : expired) {
                assertEquals(getMsgBacklog(superUserAdmin, t, consumerSubscription), 0, t);
            }
        });
        for (String t : topics) {
            assertEquals(getMsgBacklog(superUserAdmin, t, subscription), numMessages, t);
        }
        assertEquals(getMsgBacklog(superUserAdmin, topics.get(4), consumerSubscription), numMessages);

        // the same applies to the namespace clear backlog
        assertEquals(postThroughProxy(httpClient, proxyToken, consumerRole,
                "/admin/v2/namespaces/" + namespace + "/clearBacklog"), 204);
        for (String t : topics) {
            assertEquals(getMsgBacklog(superUserAdmin, t, consumerSubscription), 0, t);
            assertEquals(getMsgBacklog(superUserAdmin, t, subscription), numMessages, t);
        }

        // a tenant admin forwarded by the proxy clears all subscriptions
        assertEquals(postThroughProxy(httpClient, proxyToken, tenantAdminRole,
                "/admin/v2/namespaces/" + namespace + "/clearBacklog"), 204);
        for (String t : topics) {
            assertEquals(getMsgBacklog(superUserAdmin, t, subscription), 0, t);
        }
    }

    private static long getMsgBacklog(PulsarAdmin admin, String topic, String subscription)
            throws PulsarAdminException {
        return admin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog();
    }

    private int postThroughProxy(HttpClient httpClient, String proxyToken, String originalPrincipal, String path)
            throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(pulsar.getWebServiceAddress() + path))
                .header("Authorization", "Bearer " + proxyToken)
                .header("X-Original-Principal", originalPrincipal)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
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

    private int clearBacklogThroughProxy(HttpClient httpClient, String proxyToken, String originalPrincipal,
                                         String namespaceOrBundle, String subscription) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(pulsar.getWebServiceAddress()
                        + "/admin/v2/namespaces/" + namespaceOrBundle + "/clearBacklog/" + subscription))
                .header("Authorization", "Bearer " + proxyToken)
                .header("X-Original-Principal", originalPrincipal)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }
}
