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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import javax.naming.AuthenticationException;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class PatternTopicsConsumerImplAuthTest extends ProducerConsumerBase {
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(PatternTopicsConsumerImplAuthTest.class);
    private static final String clientRole = "pluggableRole";
    private static final String superUserRole = "superUser";
    private static final Set<String> clientAuthProviderSupportedRoles = Sets.newHashSet(clientRole);

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        // set isTcpLookup = true, to use BinaryProtoLookupService to get topics for a pattern.
        isTcpLookup = true;

        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add(superUserRole);
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(TestAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setAuthorizationProvider(TestAuthorizationProvider.class.getName());

        conf.setClusterName("test");
        super.internalSetup();
    }


    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private PulsarAdmin buildAdminClient() throws Exception {
        Authentication adminAuthentication = new ClientAuthentication(superUserRole);
        return PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl.toString())
                .authentication(adminAuthentication)
                .build();
    }

    // verify binary proto with correct auth check
    // if with invalid role, consumer for pattern topic subscription will fail
    @Test(timeOut = testTimeout)
    public void testBinaryProtoToGetTopicsOfNamespace() throws Exception {
        String key = "BinaryProtoToGetTopics";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2-" + key;
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3-" + key;
        String topicName4 = "non-persistent://my-property/my-ns/pattern-topic-4-" + key;
        Pattern pattern = Pattern.compile("my-property/my-ns/pattern-topic.*");

        @Cleanup
        PulsarAdmin admin = buildAdminClient();

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

        // 1. create partition and grant permissions
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

        admin.namespaces().grantPermissionOnNamespace("my-property/my-ns", clientRole,
                EnumSet.allOf(AuthAction.class));

        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. create producers and consumer
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
                .enableBatching(false)
                .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
                .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
                .enableBatching(false)
                .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
                .create();
        Producer<byte[]> producer4 = pulsarClient.newProducer().topic(topicName4)
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer;
        // Invalid user auth-role will be rejected by authorization service
        try {
            consumer = pulsarClientInvalidRole.newConsumer()
                    .topicsPattern(pattern)
                    .patternAutoDiscoveryPeriod(2)
                    .subscriptionName(subscriptionName)
                    .subscriptionType(SubscriptionType.Shared)
                    .receiverQueueSize(4)
                    .subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (PulsarClientException.AuthorizationException pa) {
            // Ok
        }

        // create pattern topics consumer with correct role client
        consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .patternAutoDiscoveryPeriod(2)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(4)
                .subscribe();
        assertTrue(consumer.getTopic().startsWith(PatternMultiTopicsConsumerImpl.DUMMY_TOPIC_NAME_PREFIX));

        // 4. verify consumer
        assertSame(pattern, ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern());
        List<String> topics = ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions();
        List<ConsumerImpl<byte[]>> consumers = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers();

        assertEquals(topics.size(), 6);
        assertEquals(consumers.size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
                assertEquals(consumers.get(index).getTopic(), topics.get(index)));

        ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().forEach(topic -> log.debug("getTopics topic: {}", topic));

        // 5. produce data
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
            producer4.send((messagePredicate + "producer4-" + i).getBytes());
        }

        // 6. should receive all the message
        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
        producer4.close();
    }

    public static class TestAuthorizationProvider implements AuthorizationProvider {
        public ServiceConfiguration conf;

        @Override
        public void close() throws IOException {
            // No-op
        }

        @Override
        public void initialize(ServiceConfiguration conf, PulsarResources resources) throws IOException {
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
        public CompletableFuture<Boolean> allowConsumeOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
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
            CompletableFuture<Boolean> isAuthorizedFuture;

            if (role.equals(superUserRole) || role.equals(clientRole)) {
                isAuthorizedFuture = CompletableFuture.completedFuture(true);
            } else {
                isAuthorizedFuture = CompletableFuture.completedFuture(false);
            }

            return isAuthorizedFuture;
        }

        @Override
        public Boolean allowNamespaceOperation(
                NamespaceName namespaceName, String role, NamespaceOperation operation, AuthenticationDataSource authData) {
            try {
                return allowNamespaceOperationAsync(namespaceName, role, operation, authData).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RestException(e);
            }
        }

        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(
                TopicName topic, String role, TopicOperation operation, AuthenticationDataSource authData) {
            CompletableFuture<Boolean> isAuthorizedFuture;

            if (role.equals(superUserRole) || role.equals(clientRole)) {
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
            } catch (InterruptedException | ExecutionException e) {
                throw new RestException(e);
            }
        }

        @Override
        public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topic, String role,
                                                                         PolicyName policy, PolicyOperation operation,
                                                                         AuthenticationDataSource authData) {
            CompletableFuture<Boolean> isAuthorizedFuture;

            if (role.equals(superUserRole) || role.equals(clientRole)) {
                isAuthorizedFuture = CompletableFuture.completedFuture(true);
            } else {
                isAuthorizedFuture = CompletableFuture.completedFuture(false);
            }

            return isAuthorizedFuture;
        }

        @Override
        public Boolean allowTopicPolicyOperation(TopicName topicName, String role, PolicyName policy,
                                                 PolicyOperation operation, AuthenticationDataSource authData) {
            try {
                return allowTopicPolicyOperationAsync(topicName, role, policy, operation, authData).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RestException(e);
            }
        }
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
}
