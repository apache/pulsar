/**
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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.naming.AuthenticationException;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class AuthorizationProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(AuthorizationProducerConsumerTest.class);

    private static final String clientRole = "plugbleRole";
    private static final Set<String> clientAuthProviderSupportedRoles = Sets.newHashSet(clientRole);

    protected void setup() throws Exception {

        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(TestAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("test");

        super.init();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * It verifies plugable authorization service
     *
     * <pre>
     * 1. Client passes correct authorization plugin-name + correct auth role: SUCCESS
     * 2. Client passes correct authorization plugin-name + incorrect auth-role: FAIL
     * 3. Client passes incorrect authorization plugin-name + correct auth-role: FAIL
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testProducerAndConsumerAuthorization() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProvider.class.getName());
        setup();

        Authentication adminAuthentication = new ClientAuthentication("superUser");

        @Cleanup
        PulsarAdmin admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).authentication(adminAuthentication).build());

        String lookupUrl = pulsar.getBrokerServiceUrl();

        Authentication authentication = new ClientAuthentication(clientRole);
        Authentication authenticationInvalidRole = new ClientAuthentication("test-role");

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl).authentication(authentication)
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

        @Cleanup
        PulsarClient pulsarClientInvalidRole = PulsarClient.builder().serviceUrl(lookupUrl)
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .authentication(authenticationInvalidRole).build();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

        // (1) Valid Producer and consumer creation
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic")
                .subscriptionName("my-subscriber-name").subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic")
                .create();
        consumer.close();
        producer.close();

        // (2) InValid user auth-role will be rejected by authorization service
        try {
            consumer = pulsarClientInvalidRole.newConsumer().topic("persistent://my-property/my-ns/my-topic")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (PulsarClientException.AuthorizationException pa) {
            // Ok
        }
        try {
            producer = pulsarClientInvalidRole.newProducer().topic("persistent://my-property/my-ns/my-topic")
                    .create();
            Assert.fail("should have failed with authorization error");
        } catch (PulsarClientException.AuthorizationException pa) {
            // Ok
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testSubscriberPermission() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setEnablePackagesManagement(true);
        conf.setPackagesManagementStorageProvider(MockedPackagesStorageProvider.class.getName());
        conf.setAuthorizationProvider(PulsarAuthorizationProvider.class.getName());
        setup();

        final String tenantRole = "tenant-role";
        final String subscriptionRole = "sub1-role";
        final String subscriptionName = "sub1";
        final String subscriptionName2 = "sub2";
        final String namespace = "my-property/my-ns-sub-auth";
        final String topicName = "persistent://" + namespace + "/my-topic";
        Authentication adminAuthentication = new ClientAuthentication("superUser");

        clientAuthProviderSupportedRoles.add(subscriptionRole);

        @Cleanup
        PulsarAdmin superAdmin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).authentication(adminAuthentication).build());

        Authentication tenantAdminAuthentication = new ClientAuthentication(tenantRole);
        @Cleanup
        PulsarAdmin tenantAdmin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(tenantAdminAuthentication).build());

        Authentication subAdminAuthentication = new ClientAuthentication(subscriptionRole);
        @Cleanup
        PulsarAdmin sub1Admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(subAdminAuthentication).build());

        Authentication authentication = new ClientAuthentication(subscriptionRole);

        superAdmin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        superAdmin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet(tenantRole), Sets.newHashSet("test")));
        superAdmin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        // subscriptionRole doesn't have topic-level authorization, so it will fail to get topic stats-internal info
        try {
            sub1Admin.topics().getInternalStats(topicName, true);
            fail("should have failed with authorization exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith(
                    "Unauthorized to validateTopicOperation for operation [GET_STATS]"));
        }
        try {
            sub1Admin.topics().getBacklogSizeByMessageId(topicName, MessageId.earliest);
            fail("should have failed with authorization exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith(
                    "Unauthorized to validateTopicOperation for operation"));
        }

        // grant topic consume authorization to the subscriptionRole
        tenantAdmin.topics().grantPermission(topicName, subscriptionRole,
                Collections.singleton(AuthAction.consume));

        replacePulsarClient(PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(authentication));

        // (1) Create subscription name
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName2)
                .subscribe();
        consumer.close();
        consumer2.close();

        List<String> subscriptions = sub1Admin.topics().getSubscriptions(topicName);
        assertEquals(subscriptions.size(), 2);

        // now, subscriptionRole have consume authorization on topic, so it will successfully get topic internal stats
        PersistentTopicInternalStats internalStats = sub1Admin.topics().getInternalStats(topicName, true);
        assertNotNull(internalStats);
        Long backlogSize = sub1Admin.topics().getBacklogSizeByMessageId(topicName, MessageId.earliest);
        assertEquals(backlogSize.longValue(), 0);

        // verify tenant is able to perform all subscription-admin api
        tenantAdmin.topics().skipAllMessages(topicName, subscriptionName);
        tenantAdmin.topics().skipMessages(topicName, subscriptionName, 1);
        try {
            tenantAdmin.topics().expireMessages(topicName, subscriptionName, 10);
        } catch (Exception e) {
            // my-sub1 has no msg backlog, so expire message won't be issued on that subscription
            assertTrue(e.getMessage().startsWith("Expire message by timestamp not issued on topic"));
        }
        tenantAdmin.topics().expireMessages(topicName, subscriptionName, new MessageIdImpl(-1, -1, -1), true);
        tenantAdmin.topics().peekMessages(topicName, subscriptionName, 1);
        tenantAdmin.topics().resetCursor(topicName, subscriptionName, 10);
        tenantAdmin.topics().resetCursor(topicName, subscriptionName, MessageId.earliest);

        // subscriptionRole doesn't have namespace-level authorization, so it will fail to unsubscribe namespace
        try {
            sub1Admin.namespaces().unsubscribeNamespace(namespace, subscriptionName2);
            fail("should have failed with authorization exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith(
                    "Unauthorized to validateNamespaceOperation for operation [UNSUBSCRIBE]"));
        }
        try {
            sub1Admin.namespaces().getTopics(namespace);
            fail("should have failed with authorization exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith(
                    "Unauthorized to validateNamespaceOperation for operation [GET_TOPICS]"));
        }
        try {
            sub1Admin.packages().listPackages("function", namespace);
            fail("should have failed with authorization exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith(
                    "Role sub1-role has not the 'package' permission to do the packages operations"));
        }

        // grant namespace-level authorization to the subscriptionRole
        tenantAdmin.namespaces().grantPermissionOnNamespace(namespace, subscriptionRole,
                Sets.newHashSet(AuthAction.consume, AuthAction.packages));

        // now, subscriptionRole have consume authorization on namespace, so it will successfully unsubscribe namespace
        sub1Admin.namespaces().unsubscribeNamespaceBundle(namespace, "0x00000000_0xffffffff", subscriptionName2);
        subscriptions = sub1Admin.topics().getSubscriptions(topicName);
        assertEquals(subscriptions.size(), 1);
        List<String> topics = sub1Admin.namespaces().getTopics(namespace);
        assertEquals(topics.size(), 1);
        List<String> packages = sub1Admin.packages().listPackages("function", namespace);
        assertEquals(packages.size(), 0);

        // subscriptionRole has namespace-level authorization
        sub1Admin.topics().resetCursor(topicName, subscriptionName, 10);

        // grant subscription access to specific different role and only that role can access the subscription
        String otherPrincipal = "Principal-1-to-access-sub";
        tenantAdmin.namespaces().grantPermissionOnSubscription(namespace, subscriptionName,
                Collections.singleton(otherPrincipal));
        TreeMap<String, Set<String>> permissionOnSubscription = new TreeMap<>();
        permissionOnSubscription.put(subscriptionName, Collections.singleton(otherPrincipal));
        Assert.assertEquals(tenantAdmin.namespaces().getPermissionOnSubscription(namespace), permissionOnSubscription);

        // now, subscriptionRole doesn't have subscription level access so, it will fail to access subscription
        try {
            sub1Admin.topics().resetCursor(topicName, subscriptionName, 10);
            fail("should have fail with authorization exception");
        } catch (org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException e) {
            // Ok
        }

        // reset on position
        try {
            sub1Admin.topics().resetCursor(topicName, subscriptionName, MessageId.earliest);
            fail("should have fail with authorization exception");
        } catch (org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException e) {
            // Ok
        }

        // now, grant subscription-access to subscriptionRole as well
        superAdmin.namespaces().grantPermissionOnSubscription(namespace, subscriptionName,
                Sets.newHashSet(otherPrincipal, subscriptionRole));
        TreeMap<String, Set<String>> permissionOnSubscription1 = new TreeMap<>();
        permissionOnSubscription1.put(subscriptionName, Sets.newHashSet(otherPrincipal, subscriptionRole));
        Assert.assertEquals(tenantAdmin.namespaces().getPermissionOnSubscription(namespace), permissionOnSubscription1);

        sub1Admin.topics().skipAllMessages(topicName, subscriptionName);
        sub1Admin.topics().skipMessages(topicName, subscriptionName, 1);
        try {
            tenantAdmin.topics().expireMessages(topicName, subscriptionName, 10);
        } catch (Exception e) {
            // my-sub1 has no msg backlog, so expire message won't be issued on that subscription
            assertTrue(e.getMessage().startsWith("Expire message by timestamp not issued on topic"));
        }        sub1Admin.topics().peekMessages(topicName, subscriptionName, 1);
        sub1Admin.topics().resetCursor(topicName, subscriptionName, 10);
        sub1Admin.topics().resetCursor(topicName, subscriptionName, MessageId.earliest);

        superAdmin.namespaces().revokePermissionOnSubscription(namespace, subscriptionName, subscriptionRole);

        try {
            sub1Admin.topics().resetCursor(topicName, subscriptionName, 10);
            fail("should have fail with authorization exception");
        } catch (org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException e) {
            // Ok
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testClearBacklogPermission() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(PulsarAuthorizationProvider.class.getName());
        setup();

        final String tenantRole = "tenant-role";
        final String subscriptionRole = "sub-role";
        final String subscriptionName = "sub1";
        final String namespace = "my-property/my-ns-sub-auth";
        final String topicName = "persistent://" + namespace + "/my-topic";
        Authentication adminAuthentication = new ClientAuthentication("superUser");

        clientAuthProviderSupportedRoles.add(subscriptionRole);

        @Cleanup
        PulsarAdmin superAdmin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(adminAuthentication).build());

        Authentication tenantAdminAuthentication = new ClientAuthentication(tenantRole);
        @Cleanup
        PulsarAdmin tenantAdmin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(tenantAdminAuthentication).build());

        Authentication subAdminAuthentication = new ClientAuthentication(subscriptionRole);
        @Cleanup
        PulsarAdmin sub1Admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(subAdminAuthentication).build());

        superAdmin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        superAdmin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet(tenantRole), Sets.newHashSet("test")));
        superAdmin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        superAdmin.topics().createPartitionedTopic(topicName, 1);
        assertEquals(tenantAdmin.topics().getPartitionedTopicList(namespace),
                Lists.newArrayList(topicName));

        // grant topic consume&produce authorization to the subscriptionRole
        superAdmin.topics().grantPermission(topicName, subscriptionRole,
                Sets.newHashSet(AuthAction.produce, AuthAction.consume));
        replacePulsarClient(PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(subAdminAuthentication));

        @Cleanup
        Producer<byte[]> batchProducer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subscriptionName)
                .subscribe();

        CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
        for (int i = 0; i < 10; i++) {
            completableFuture = batchProducer.sendAsync("a".getBytes());
        }
        completableFuture.get();
        assertEquals(sub1Admin.topics().getStats(topicName + "-partition-0").getSubscriptions()
                .get(subscriptionName).getMsgBacklog(), 10);

        // subscriptionRole doesn't have namespace-level authorization, so it will fail to clear backlog
        try {
            sub1Admin.topics().getPartitionedTopicList(namespace);
            fail("should have failed with authorization exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith(
                    "Unauthorized to validateNamespaceOperation for operation [GET_TOPICS]"));
        }
        try {
            sub1Admin.namespaces().clearNamespaceBundleBacklog(namespace, "0x00000000_0xffffffff");
            fail("should have failed with authorization exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith(
                    "Unauthorized to validateNamespaceOperation for operation [CLEAR_BACKLOG]"));
        }

        superAdmin.namespaces().grantPermissionOnNamespace(namespace, subscriptionRole,
                Sets.newHashSet(AuthAction.consume));
        // now, subscriptionRole have consume authorization on namespace, so it will successfully clear backlog
        assertEquals(sub1Admin.topics().getPartitionedTopicList(namespace),
                Lists.newArrayList(topicName));
        sub1Admin.namespaces().clearNamespaceBundleBacklog(namespace, "0x00000000_0xffffffff");
        assertEquals(sub1Admin.topics().getStats(topicName + "-partition-0").getSubscriptions()
                .get(subscriptionName).getMsgBacklog(), 0);

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testSubscriptionPrefixAuthorization() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProviderWithSubscriptionPrefix.class.getName());
        setup();

        Authentication adminAuthentication = new ClientAuthentication("superUser");
        @Cleanup
        PulsarAdmin admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).authentication(adminAuthentication).build());

        Authentication authentication = new ClientAuthentication(clientRole);

        replacePulsarClient(PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(authentication));


        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("prop-prefix",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop-prefix/ns", Sets.newHashSet("test"));

        // (1) Valid subscription name will be approved by authorization service
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://prop-prefix/ns/t1")
                .subscriptionName(clientRole + "-sub1").subscribe();
        consumer.close();

        // (2) InValid subscription name will be rejected by authorization service
        try {
            consumer = pulsarClient.newConsumer().topic("persistent://prop-prefix/ns/t1").subscriptionName("sub1")
                    .subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (PulsarClientException.AuthorizationException pa) {
            // Ok
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGrantPermission() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProviderWithGrantPermission.class.getName());
        setup();

        AuthorizationService authorizationService = new AuthorizationService(conf, null);
        TopicName topicName = TopicName.get("persistent://prop/cluster/ns/t1");
        String role = "test-role";
        Assert.assertFalse(authorizationService.canProduce(topicName, role, null));
        Assert.assertFalse(authorizationService.canConsume(topicName, role, null, "sub1"));
        authorizationService.grantPermissionAsync(topicName, null, role, "auth-json").get();
        Assert.assertTrue(authorizationService.canProduce(topicName, role, null));
        Assert.assertTrue(authorizationService.canConsume(topicName, role, null, "sub1"));

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testAuthData() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProviderWithGrantPermission.class.getName());
        setup();

        AuthorizationService authorizationService = new AuthorizationService(conf, null);
        TopicName topicName = TopicName.get("persistent://prop/cluster/ns/t1");
        String role = "test-role";
        authorizationService.grantPermissionAsync(topicName, null, role, "auth-json").get();
        Assert.assertEquals(TestAuthorizationProviderWithGrantPermission.authDataJson, "auth-json");
        Assert.assertTrue(authorizationService.canProduce(topicName, role, new AuthenticationDataCommand("prod-auth")));
        Assert.assertEquals(TestAuthorizationProviderWithGrantPermission.authenticationData.getCommandData(),
                "prod-auth");
        Assert.assertTrue(
                authorizationService.canConsume(topicName, role, new AuthenticationDataCommand("cons-auth"), "sub1"));
        Assert.assertEquals(TestAuthorizationProviderWithGrantPermission.authenticationData.getCommandData(),
                "cons-auth");

        log.info("-- Exiting {} test --", methodName);
    }

    public static class ClientAuthentication implements Authentication {
        String user;

        public ClientAuthentication(String user) {
            this.user = user;
        }

        @Override
        public void close() throws IOException {
            // No-op
        }

        @Override
        public String getAuthMethodName() {
            return "test";
        }

        @Override
        public AuthenticationDataProvider getAuthData() throws PulsarClientException {
            AuthenticationDataProvider provider = new AuthenticationDataProvider() {
                public boolean hasDataForHttp() {
                    return true;
                }

                @SuppressWarnings("unchecked")
                public Set<Map.Entry<String, String>> getHttpHeaders() {
                    return Sets.newHashSet(Maps.immutableEntry("user", user));
                }

                public boolean hasDataFromCommand() {
                    return true;
                }

                public String getCommandData() {
                    return user;
                }
            };
            return provider;
        }

        @Override
        public void configure(Map<String, String> authParams) {
            // No-op
        }

        @Override
        public void start() throws PulsarClientException {
            // No-op
        }

    }

    public static class TestAuthenticationProvider implements AuthenticationProvider {

        @Override
        public void close() throws IOException {
            // no-op
        }

        @Override
        public void initialize(ServiceConfiguration config) throws IOException {
            // No-op
        }

        @Override
        public String getAuthMethodName() {
            return "test";
        }

        @Override
        public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
            return authData.getCommandData() != null ? authData.getCommandData() : authData.getHttpHeader("user");
        }

    }

    public static class TestAuthorizationProvider implements AuthorizationProvider {

        public ServiceConfiguration conf;

        @Override
        public void close() throws IOException {
            // No-op
        }

        @Override
        public CompletableFuture<Boolean> isSuperUser(String role,
                                                      ServiceConfiguration serviceConfiguration) {
            Set<String> superUserRoles = serviceConfiguration.getSuperUserRoles();
            return CompletableFuture.completedFuture(role != null && superUserRoles.contains(role) ? true : false);
        }

        @Override
        public void initialize(ServiceConfiguration conf, PulsarResources pulsarResources) throws IOException {
            this.conf = conf;
            // No-op
        }

        @Override
        public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(clientAuthProviderSupportedRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            return CompletableFuture.completedFuture(clientAuthProviderSupportedRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(clientAuthProviderSupportedRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
            return null;
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions,
                String role, String authenticationData) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(TopicName topicname, Set<AuthAction> actions, String role,
                String authenticationData) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace,
                String subscriptionName, Set<String> roles, String authDataJson) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace,
                String subscriptionName, String role, String authDataJson) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo, AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> allowTenantOperationAsync(
            String tenantName, String role, TenantOperation operation, AuthenticationDataSource authData) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public Boolean allowTenantOperation(
            String tenantName, String role, TenantOperation operation, AuthenticationDataSource authData) {
            return true;
        }

        @Override
        public CompletableFuture<Boolean> allowNamespaceOperationAsync(
            NamespaceName namespaceName, String role, NamespaceOperation operation, AuthenticationDataSource authData) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public Boolean allowNamespaceOperation(
            NamespaceName namespaceName, String role, NamespaceOperation operation, AuthenticationDataSource authData) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(
            TopicName topic, String role, TopicOperation operation, AuthenticationDataSource authData) {
            CompletableFuture<Boolean> isAuthorizedFuture;

            if (role.equals("plugbleRole")) {
                isAuthorizedFuture = CompletableFuture.completedFuture(true);
            } else {
                isAuthorizedFuture = CompletableFuture.completedFuture(false);
            }

            return isAuthorizedFuture;
        }

        @Override
        public Boolean allowTopicOperation(
            TopicName topicName, String role, TopicOperation operation, AuthenticationDataSource authData) {
            try {
                return allowTopicOperationAsync(topicName, role, operation, authData).get();
            } catch (InterruptedException e) {
                throw new RestException(e);
            } catch (ExecutionException e) {
                throw new RestException(e);
            }
        }
    }

    /**
     * This provider always fails authorization on consumer and passes on producer
     *
     */
    public static class TestAuthorizationProvider2 extends TestAuthorizationProvider {

        @Override
        public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(true);
        }
    }

    public static class TestAuthorizationProviderWithSubscriptionPrefix extends TestAuthorizationProvider {
        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic,
                                                                   String role,
                                                                   TopicOperation operation,
                                                                   AuthenticationDataSource authData) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            if (authData.hasSubscription()) {
                String subscription = authData.getSubscription();
                if (isNotBlank(subscription)) {
                    if (!subscription.startsWith(role)) {
                        future.completeExceptionally(new PulsarServerException(
                                "The subscription name needs to be prefixed by the authentication role"));
                    }
                }
            }
            future.complete(clientRole.equals(role));
            return future;
        }
    }

    public static class TestAuthorizationProviderWithGrantPermission extends TestAuthorizationProvider {

        private Set<String> grantRoles = Sets.newHashSet();
        static AuthenticationDataSource authenticationData;
        static String authDataJson;

        @Override
        public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            this.authenticationData = authenticationData;
            return CompletableFuture.completedFuture(grantRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            this.authenticationData = authenticationData;
            return CompletableFuture.completedFuture(grantRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            this.authenticationData = authenticationData;
            return CompletableFuture.completedFuture(grantRoles.contains(role));
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions,
                String role, String authData) {
            this.authDataJson = authData;
            grantRoles.add(role);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(TopicName topicname, Set<AuthAction> actions, String role,
                String authData) {
            this.authDataJson = authData;
            grantRoles.add(role);
            return CompletableFuture.completedFuture(null);
        }
    }

}
