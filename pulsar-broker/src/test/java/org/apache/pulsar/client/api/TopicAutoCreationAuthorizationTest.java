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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.collect.Sets;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationDataForwarded;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.namespace.TopicExistsInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.ClientAuthentication;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.TestAuthenticationProvider;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.TestAuthorizationProvider;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.auth.v5.V5AuthenticationLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Topic auto-creation goes through {@code AuthorizationProvider#allowTopicAutoCreationAsync}.
 *
 * <p>Runs its own broker rather than {@code SharedPulsarBaseTest}, because it needs authentication and a custom
 * authorization provider.
 */
@Test(groups = "broker-api")
public class TopicAutoCreationAuthorizationTest extends ProducerConsumerBase {

    private static final String CREATOR_ROLE = "creator";
    private static final String USER_ROLE = "user";
    private static final String PROXY_ROLE = "proxy";
    private static final String NAMESPACE = "my-property/my-ns";
    /** A namespace whose auto topic creation creates partitioned topics. */
    private static final String PARTITIONED_NAMESPACE = "my-property/partitioned-ns";
    private static final int DEFAULT_PARTITIONS = 2;

    private PulsarAdmin superAdmin;

    /** Topics whose name contains it make the auto-creation check fail, as if the provider's backend were down. */
    private static final String PROVIDER_FAILURE = "provider-failure";

    /**
     * Lets the client roles and the proxy produce, consume and look up, but only {@link #CREATOR_ROLE} and the proxy
     * trigger topic auto-creation.
     */
    public static class AutoCreationAuthorizationProvider extends TestAuthorizationProvider {

        private static final Set<String> CLIENT_ROLES = Set.of(CREATOR_ROLE, USER_ROLE, PROXY_ROLE);

        /** The auth data each role was last checked with by {@link #allowTopicAutoCreationAsync}. */
        static final Map<String, AuthenticationDataSource> AUTO_CREATION_AUTH_DATA = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String role,
                                                                   TopicOperation operation,
                                                                   AuthenticationDataSource authData) {
            return CompletableFuture.completedFuture(CLIENT_ROLES.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> allowTopicAutoCreationAsync(TopicName topic, String role,
                                                                      AuthenticationDataSource authData) {
            if (authData != null) {
                AUTO_CREATION_AUTH_DATA.put(role, authData);
            }
            if (topic.toString().contains(PROVIDER_FAILURE)) {
                return CompletableFuture.failedFuture(new IllegalStateException("Authorization backend is down"));
            }
            return CompletableFuture.completedFuture(CREATOR_ROLE.equals(role) || PROXY_ROLE.equals(role));
        }
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setSuperUserRoles(Set.of("superUser"));
        conf.setProxyRoles(Set.of(PROXY_ROLE));
        conf.setAuthenticationProviders(Set.of(TestAuthenticationProvider.class.getName()));
        conf.setAuthorizationProvider(AutoCreationAuthorizationProvider.class.getName());
        conf.setAllowAutoTopicCreation(true);
        conf.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setClusterName("test");
        super.init();

        superAdmin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(new ClientAuthentication("superUser")).build();
        superAdmin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        superAdmin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet("test")));
        superAdmin.namespaces().createNamespace(NAMESPACE, Sets.newHashSet("test"));
        // Set at creation: the test authorization provider supports no namespace policy operations.
        Policies partitionedPolicies = new Policies();
        partitionedPolicies.replication_clusters = Sets.newHashSet("test");
        partitionedPolicies.autoTopicCreationOverride = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType(TopicType.PARTITIONED.toString())
                .defaultNumPartitions(DEFAULT_PARTITIONS)
                .build();
        superAdmin.namespaces().createNamespace(PARTITIONED_NAMESPACE, partitionedPolicies);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (superAdmin != null) {
            superAdmin.close();
            superAdmin = null;
        }
        super.internalCleanup();
    }

    @DataProvider
    public Object[][] lookupTransports() {
        return new Object[][]{{"binary"}, {"http"}};
    }

    private String serviceUrl(String lookupTransport) {
        return "http".equals(lookupTransport) ? brokerUrl.toString() : pulsar.getBrokerServiceUrl();
    }

    private PulsarClient newClient(String role, String serviceUrl) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(serviceUrl)
                .authentication(new ClientAuthentication(role))
                .operationTimeout(3, TimeUnit.SECONDS)
                .build();
    }

    private boolean topicExists(String topic) throws Exception {
        TopicExistsInfo info = pulsar.getNamespaceService().checkTopicExistsAsync(TopicName.get(topic)).get();
        try {
            return info.isExists();
        } finally {
            info.recycle();
        }
    }

    @Test(timeOut = 60000)
    public void testProducerAutoCreationFollowsAuthorization() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/producer-auto-creation";
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, pulsar.getBrokerServiceUrl());
        @Cleanup
        PulsarClient creatorClient = newClient(CREATOR_ROLE, pulsar.getBrokerServiceUrl());

        assertThatThrownBy(() -> userClient.newProducer().topic(topic).create())
                .as("a producer of a role that may not create topics")
                .isInstanceOf(PulsarClientException.TopicDoesNotExistException.class);
        assertThat(topicExists(topic)).as("topic created by a refused producer").isFalse();

        creatorClient.newProducer().topic(topic).create().close();
        assertThat(topicExists(topic)).as("topic created by an allowed producer").isTrue();

        // Once the topic exists, a role that may not create topics uses it as before.
        userClient.newProducer().topic(topic).create().close();
    }

    @Test(timeOut = 60000)
    public void testConsumerAutoCreationFollowsAuthorization() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/consumer-auto-creation";
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, pulsar.getBrokerServiceUrl());
        @Cleanup
        PulsarClient creatorClient = newClient(CREATOR_ROLE, pulsar.getBrokerServiceUrl());

        assertThatThrownBy(() -> userClient.newConsumer().topic(topic).subscriptionName("sub").subscribe())
                .as("a consumer of a role that may not create topics").isInstanceOf(PulsarClientException.class);
        assertThat(topicExists(topic)).as("topic created by a refused consumer").isFalse();

        creatorClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();
        assertThat(topicExists(topic)).as("topic created by an allowed consumer").isTrue();

        userClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();
    }

    @Test(timeOut = 60000)
    public void testHttpLookupAutoCreationFollowsAuthorization() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/http-lookup-auto-creation";
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, brokerUrl.toString());
        @Cleanup
        PulsarClient creatorClient = newClient(CREATOR_ROLE, brokerUrl.toString());

        assertThatThrownBy(() -> userClient.newProducer().topic(topic).create())
                .as("a producer looking up over HTTP with a role that may not create topics")
                .isInstanceOf(PulsarClientException.class);
        assertThat(topicExists(topic)).as("topic created by a refused HTTP lookup").isFalse();

        creatorClient.newProducer().topic(topic).create().close();
        assertThat(topicExists(topic)).as("topic created by an allowed HTTP lookup").isTrue();
    }

    @Test(dataProvider = "lookupTransports", timeOut = 60000)
    public void testPartitionedAutoCreationFollowsAuthorization(String lookupTransport) throws Exception {
        String topic = "persistent://" + PARTITIONED_NAMESPACE + "/partitioned-auto-creation-" + lookupTransport;
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, serviceUrl(lookupTransport));
        @Cleanup
        PulsarClient creatorClient = newClient(CREATOR_ROLE, serviceUrl(lookupTransport));

        assertThatThrownBy(() -> userClient.newProducer().topic(topic).create())
                .as("a producer of a role that may not create partitioned topics")
                .isInstanceOf(PulsarClientException.class);
        assertThat(topicExists(topic)).as("partitioned topic created by a refused producer").isFalse();

        creatorClient.newProducer().topic(topic).create().close();
        assertThat(pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(TopicName.get(topic)).get()
                .partitions).as("partitions of the auto-created topic").isEqualTo(DEFAULT_PARTITIONS);
    }

    /**
     * The partitions of a partitioned topic an admin created are not new topics: a role that may not trigger
     * auto-creation still gets them created when they are missing, e.g. after inactive-topic deletion removed them.
     */
    @Test(dataProvider = "lookupTransports", timeOut = 60000)
    public void testMissingPartitionsOfExistingPartitionedTopicAreCreated(String lookupTransport) throws Exception {
        String topic = "persistent://" + NAMESPACE + "/existing-partitioned-" + lookupTransport;
        // Only the partitioned-topic metadata, without its partitions, as if they had all been deleted.
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopicAsync(TopicName.get(topic), new PartitionedTopicMetadata(DEFAULT_PARTITIONS))
                .get();
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, serviceUrl(lookupTransport));

        userClient.newProducer().topic(topic).create().close();
        userClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();
        for (int i = 0; i < DEFAULT_PARTITIONS; i++) {
            String partition = TopicName.get(topic).getPartition(i).toString();
            assertThat(topicExists(partition)).as("partition %s created for its existing partitioned topic", i)
                    .isTrue();
        }
    }

    @Test(timeOut = 60000)
    public void testAdminCreateSubscriptionAutoCreationFollowsAuthorization() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/admin-subscription-auto-creation";
        @Cleanup
        PulsarAdmin userAdmin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(new ClientAuthentication(USER_ROLE)).build();
        @Cleanup
        PulsarAdmin creatorAdmin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(new ClientAuthentication(CREATOR_ROLE)).build();

        assertThatThrownBy(() -> userAdmin.topics().createSubscription(topic, "sub", MessageId.latest))
                .as("a subscription created over the admin API by a role that may not create topics")
                .isInstanceOf(PulsarAdminException.class);
        assertThat(topicExists(topic)).as("topic created by a refused admin subscription").isFalse();

        creatorAdmin.topics().createSubscription(topic, "sub", MessageId.latest);
        assertThat(topicExists(topic)).as("topic created by an allowed admin subscription").isTrue();
    }

    /**
     * Using a topic that exists does not depend on the auto-creation check, even when it fails.
     */
    @Test(dataProvider = "lookupTransports", timeOut = 60000)
    public void testExistingTopicDoesNotDependOnAutoCreationCheck(String lookupTransport) throws Exception {
        String topic = "persistent://" + NAMESPACE + "/" + PROVIDER_FAILURE + "-existing-" + lookupTransport;
        // Created in the metadata, as the admin API does: the test authorization provider lets no admin do it.
        pulsar.getPulsarResources().getTopicResources().createPersistentTopicAsync(TopicName.get(topic)).get();
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, serviceUrl(lookupTransport));

        userClient.newProducer().topic(topic).create().close();
        userClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();
    }

    /**
     * A proxied HTTP lookup is refused when the original principal may not create the topic, although the proxy
     * may.
     */
    @Test(timeOut = 60000)
    public void testProxiedHttpLookupAutoCreationFollowsOriginalPrincipal() throws Exception {
        URI lookupUri = URI.create(brokerUrl.toString().replaceAll("/$", "")
                + "/lookup/v2/topic/persistent/" + NAMESPACE + "/proxied-http-lookup");
        HttpClient httpClient = HttpClient.newHttpClient();
        AutoCreationAuthorizationProvider.AUTO_CREATION_AUTH_DATA.clear();

        assertThat(proxiedLookupStatus(httpClient, lookupUri, USER_ROLE))
                .as("status of a proxied lookup for a role that may not create topics").isEqualTo(404);
        // The original principal is checked with its own auth data, not with the proxy's credentials.
        assertThat(AutoCreationAuthorizationProvider.AUTO_CREATION_AUTH_DATA.get(USER_ROLE))
                .as("auth data the original principal was checked with")
                .isInstanceOf(AuthenticationDataForwarded.class);
        assertThat(AutoCreationAuthorizationProvider.AUTO_CREATION_AUTH_DATA.get(PROXY_ROLE))
                .as("auth data the proxy was checked with")
                .isNotNull()
                .isNotInstanceOf(AuthenticationDataForwarded.class);
        assertThat(proxiedLookupStatus(httpClient, lookupUri, CREATOR_ROLE))
                .as("status of a proxied lookup for a role that may create topics").isEqualTo(200);
    }

    private static int proxiedLookupStatus(HttpClient httpClient, URI lookupUri, String originalPrincipal)
            throws Exception {
        HttpRequest request = HttpRequest.newBuilder(lookupUri)
                .header("user", PROXY_ROLE)
                .header("X-Original-Principal", originalPrincipal)
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    /**
     * Auto-creating a scalable topic on lookup ({@code DagWatchSession}) follows the authorization provider too.
     */
    @Test(timeOut = 60000)
    public void testScalableTopicAutoCreationFollowsAuthorization() throws Exception {
        String topic = "topic://" + NAMESPACE + "/scalable-auto-creation";
        @Cleanup
        org.apache.pulsar.client.api.v5.PulsarClient userClient = newV5Client(USER_ROLE);
        @Cleanup
        org.apache.pulsar.client.api.v5.PulsarClient creatorClient = newV5Client(CREATOR_ROLE);

        assertThatThrownBy(() -> userClient.newProducer(Schema.string()).topic(topic).create())
                .as("a producer of a role that may not create scalable topics")
                .isInstanceOf(org.apache.pulsar.client.api.v5.PulsarClientException.NotFoundException.class);
        assertThat(scalableTopicExists(topic)).as("scalable topic created by a refused producer").isFalse();

        creatorClient.newProducer(Schema.string()).topic(topic).create().close();
        assertThat(scalableTopicExists(topic)).as("scalable topic created by an allowed producer").isTrue();
    }

    private org.apache.pulsar.client.api.v5.PulsarClient newV5Client(String role) throws Exception {
        return org.apache.pulsar.client.api.v5.PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .authentication(V5AuthenticationLoader.forStartedV4Plugin(new ClientAuthentication(role)))
                .build();
    }

    private boolean scalableTopicExists(String topic) throws Exception {
        return pulsar.getPulsarResources().getScalableTopicResources()
                .getScalableTopicMetadataAsync(TopicName.get(topic)).get().isPresent();
    }
}
