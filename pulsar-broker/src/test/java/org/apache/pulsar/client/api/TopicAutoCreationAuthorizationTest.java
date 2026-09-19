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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.namespace.TopicExistsInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.ClientAuthentication;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.TestAuthenticationProvider;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.TestAuthorizationProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Topic auto-creation goes through {@code AuthorizationProvider#allowTopicAutoCreationAsync}.
 */
@Test(groups = "broker-api")
public class TopicAutoCreationAuthorizationTest extends ProducerConsumerBase {

    private static final String CREATOR_ROLE = "creator";
    private static final String USER_ROLE = "user";
    private static final String NAMESPACE = "my-property/my-ns";

    private TopicType autoCreationType = TopicType.NON_PARTITIONED;
    private PulsarAdmin superAdmin;

    /**
     * Lets both roles produce, consume and look up, but only {@link #CREATOR_ROLE} trigger topic auto-creation.
     */
    public static class AutoCreationAuthorizationProvider extends TestAuthorizationProvider {

        private static final Set<String> CLIENT_ROLES = Set.of(CREATOR_ROLE, USER_ROLE);

        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String role,
                                                                   TopicOperation operation,
                                                                   AuthenticationDataSource authData) {
            return CompletableFuture.completedFuture(CLIENT_ROLES.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> allowTopicAutoCreationAsync(TopicName topic, String role,
                                                                      AuthenticationDataSource authData) {
            return CompletableFuture.completedFuture(CREATOR_ROLE.equals(role));
        }
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setSuperUserRoles(Set.of("superUser"));
        conf.setAuthenticationProviders(Set.of(TestAuthenticationProvider.class.getName()));
        conf.setAuthorizationProvider(AutoCreationAuthorizationProvider.class.getName());
        conf.setAllowAutoTopicCreation(true);
        conf.setAllowAutoTopicCreationType(autoCreationType);
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setClusterName("test");
        super.init();

        superAdmin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(new ClientAuthentication("superUser")).build();
        superAdmin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        superAdmin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet("test")));
        superAdmin.namespaces().createNamespace(NAMESPACE, Sets.newHashSet("test"));
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

    @Test
    public void testProducerAutoCreationFollowsAuthorization() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/producer-auto-creation";
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, pulsar.getBrokerServiceUrl());
        @Cleanup
        PulsarClient creatorClient = newClient(CREATOR_ROLE, pulsar.getBrokerServiceUrl());

        assertThrows(PulsarClientException.class, () -> userClient.newProducer().topic(topic).create());
        assertFalse(topicExists(topic));

        creatorClient.newProducer().topic(topic).create().close();
        assertTrue(topicExists(topic));

        // Once the topic exists, a role that may not create topics uses it as before.
        userClient.newProducer().topic(topic).create().close();
    }

    @Test
    public void testConsumerAutoCreationFollowsAuthorization() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/consumer-auto-creation";
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, pulsar.getBrokerServiceUrl());
        @Cleanup
        PulsarClient creatorClient = newClient(CREATOR_ROLE, pulsar.getBrokerServiceUrl());

        assertThrows(PulsarClientException.class,
                () -> userClient.newConsumer().topic(topic).subscriptionName("sub").subscribe());
        assertFalse(topicExists(topic));

        creatorClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();
        assertTrue(topicExists(topic));

        userClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();
    }

    @Test
    public void testHttpLookupAutoCreationFollowsAuthorization() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/http-lookup-auto-creation";
        @Cleanup
        PulsarClient userClient = newClient(USER_ROLE, brokerUrl.toString());
        @Cleanup
        PulsarClient creatorClient = newClient(CREATOR_ROLE, brokerUrl.toString());

        assertThrows(PulsarClientException.class, () -> userClient.newProducer().topic(topic).create());
        assertFalse(topicExists(topic));

        creatorClient.newProducer().topic(topic).create().close();
        assertTrue(topicExists(topic));
    }

    @Test
    public void testPartitionedAutoCreationFollowsAuthorization() throws Exception {
        cleanup();
        autoCreationType = TopicType.PARTITIONED;
        try {
            setup();
            String topic = "persistent://" + NAMESPACE + "/partitioned-auto-creation";
            @Cleanup
            PulsarClient userClient = newClient(USER_ROLE, pulsar.getBrokerServiceUrl());
            @Cleanup
            PulsarClient creatorClient = newClient(CREATOR_ROLE, pulsar.getBrokerServiceUrl());

            assertThrows(PulsarClientException.class, () -> userClient.newProducer().topic(topic).create());
            assertFalse(topicExists(topic));

            creatorClient.newProducer().topic(topic).create().close();
            assertEquals(pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(TopicName.get(topic)).get()
                    .partitions, conf.getDefaultNumPartitions());
        } finally {
            autoCreationType = TopicType.NON_PARTITIONED;
        }
    }
}
