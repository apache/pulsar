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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import io.jsonwebtoken.Jwts;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Namespace clear backlog operations for replicator cursor names on requests forwarded by a proxy.
 */
@Test(groups = "broker-admin")
public class NamespaceSubscriptionProxyRoleTest extends MockedPulsarStandalone {

    private static final String DEFAULT_BUNDLE = "0x00000000_0xffffffff";
    private static final String ADMIN_PROXY_ROLE = "tenant-admin-proxy";
    private static final String SUPER_USER_PROXY_ROLE = "super-user-proxy";
    private static final String TENANT_ADMIN_ROLE = "tenant-admin";
    private static final String CONSUMER_ROLE = "consumer";

    private PulsarAdmin superUserAdmin;
    private HttpClient httpClient;

    @SneakyThrows
    @BeforeClass
    public void setup() {
        getServiceConfiguration().setDefaultNumberOfNamespaceBundles(1);
        getServiceConfiguration().setForceDeleteNamespaceAllowed(true);
        configureTokenAuthentication();
        configureDefaultAuthorization();
        Set<String> superUserRoles = new HashSet<>(getServiceConfiguration().getSuperUserRoles());
        superUserRoles.add(SUPER_USER_PROXY_ROLE);
        getServiceConfiguration().setSuperUserRoles(superUserRoles);
        getServiceConfiguration().setProxyRoles(Set.of(ADMIN_PROXY_ROLE, SUPER_USER_PROXY_ROLE));
        start();
        superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        httpClient = HttpClient.newHttpClient();
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        httpClient = null;
        if (superUserAdmin != null) {
            superUserAdmin.close();
            superUserAdmin = null;
        }
        close();
    }

    @Test
    public void testReplicatorCursorNameThroughProxy() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String tenant = "tenant-" + random;
        final String namespace = tenant + "/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String subscription = "sub-" + random;
        // resolved to the subscription with the same name as the remote cluster part of the name
        final String replicatorCursor =
                getPulsarService().getConfiguration().getReplicatorPrefix() + "." + subscription;
        final int numMessages = 5;
        superUserAdmin.tenants().createTenant(tenant, TenantInfo.builder()
                .adminRoles(Set.of(ADMIN_PROXY_ROLE, TENANT_ADMIN_ROLE))
                .allowedClusters(Set.of(getPulsarService().getConfiguration().getClusterName()))
                .build());
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, CONSUMER_ROLE, Set.of(AuthAction.consume));
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createSubscription(topic, subscription, MessageId.earliest);
        @Cleanup
        Producer<byte[]> producer = getPulsarService().getClient().newProducer().topic(topic)
                .enableBatching(false)
                .create();
        sendMessages(producer, numMessages);

        // a tenant admin proxy forwarding a consumer: the consumer is not allowed to use replicator cursor names
        assertEquals(clearBacklog(ADMIN_PROXY_ROLE, CONSUMER_ROLE, namespace, null, replicatorCursor), 403);
        assertEquals(clearBacklog(ADMIN_PROXY_ROLE, CONSUMER_ROLE, namespace, DEFAULT_BUNDLE, replicatorCursor),
                403);
        assertEquals(getMsgBacklog(topic, subscription), numMessages);

        // a super user proxy forwarding a tenant admin keeps the existing behaviour
        assertEquals(clearBacklog(SUPER_USER_PROXY_ROLE, TENANT_ADMIN_ROLE, namespace, null, replicatorCursor),
                204);
        assertEquals(getMsgBacklog(topic, subscription), 0);
        sendMessages(producer, numMessages);
        assertEquals(clearBacklog(SUPER_USER_PROXY_ROLE, TENANT_ADMIN_ROLE, namespace, DEFAULT_BUNDLE,
                replicatorCursor), 204);
        assertEquals(getMsgBacklog(topic, subscription), 0);

        producer.close();
        superUserAdmin.namespaces().deleteNamespace(namespace, true);
        superUserAdmin.tenants().deleteTenant(tenant);
    }

    private int clearBacklog(String proxyRole, String originalPrincipal, String namespace, String bundle,
                             String subscription) throws Exception {
        final String proxyToken = Jwts.builder().claim("sub", proxyRole).signWith(SECRET_KEY).compact();
        final String path = "/admin/v2/namespaces/" + namespace + (bundle != null ? "/" + bundle : "")
                + "/clearBacklog/" + subscription;
        HttpRequest request = HttpRequest.newBuilder(URI.create(getPulsarService().getWebServiceAddress() + path))
                .header("Authorization", "Bearer " + proxyToken)
                .header("X-Original-Principal", originalPrincipal)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static void sendMessages(Producer<byte[]> producer, int numMessages) throws Exception {
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes());
        }
    }

    private long getMsgBacklog(String topic, String subscription) throws PulsarAdminException {
        return superUserAdmin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog();
    }
}
