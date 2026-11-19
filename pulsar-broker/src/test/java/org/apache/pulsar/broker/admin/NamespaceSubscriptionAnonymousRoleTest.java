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
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Namespace and topic subscription operations performed by the configured anonymous role.
 */
@Test(groups = "broker-admin")
public class NamespaceSubscriptionAnonymousRoleTest extends MockedPulsarStandalone {

    private static final String ANONYMOUS_ROLE = "anonymous-role";
    private static final String DEFAULT_BUNDLE = "0x00000000_0xffffffff";

    private PulsarAdmin superUserAdmin;
    private PulsarAdmin anonymousAdmin;

    @SneakyThrows
    @BeforeClass
    public void setup() {
        getServiceConfiguration().setDefaultNumberOfNamespaceBundles(1);
        getServiceConfiguration().setForceDeleteNamespaceAllowed(true);
        configureTokenAuthentication();
        configureDefaultAuthorization();
        getServiceConfiguration().setAnonymousUserRole(ANONYMOUS_ROLE);
        start();
        superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        anonymousAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .build();
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        if (anonymousAdmin != null) {
            anonymousAdmin.close();
            anonymousAdmin = null;
        }
        if (superUserAdmin != null) {
            superUserAdmin.close();
            superUserAdmin = null;
        }
        close();
    }

    @Test
    public void testNamespaceSubscriptionOperationsApplySubscriptionPolicies() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String otherSub = "other-sub";
        final String ownSub = ANONYMOUS_ROLE + "-sub";
        final int numMessages = 5;
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createSubscription(topic, otherSub, MessageId.earliest);
        superUserAdmin.topics().createSubscription(topic, ownSub, MessageId.earliest);
        @Cleanup
        Producer<byte[]> producer = getPulsarService().getClient().newProducer().topic(topic)
                .enableBatching(false)
                .create();
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes());
        }
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, ANONYMOUS_ROLE, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);

        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> anonymousAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> anonymousAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace,
                        DEFAULT_BUNDLE, otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> anonymousAdmin.namespaces().unsubscribeNamespace(namespace, otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> anonymousAdmin.namespaces().unsubscribeNamespaceBundle(namespace, DEFAULT_BUNDLE, otherSub));
        assertEquals(getMsgBacklog(topic, otherSub), numMessages);

        anonymousAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, ownSub);
        assertEquals(getMsgBacklog(topic, ownSub), 0);

        // clearing the backlog of all subscriptions only clears the subscriptions the role may access
        sendMessages(producer, numMessages);
        anonymousAdmin.namespaces().clearNamespaceBacklog(namespace);
        assertEquals(getMsgBacklog(topic, ownSub), 0);
        assertEquals(getMsgBacklog(topic, otherSub), 2 * numMessages);

        producer.close();
        superUserAdmin.namespaces().deleteNamespace(namespace, true);
    }

    @Test
    public void testTopicSubscriptionOperationsApplySubscriptionPolicies() throws Exception {
        final String random = UUID.randomUUID().toString();
        final String namespace = "public/" + random;
        final String topic = "persistent://" + namespace + "/" + random;
        final String otherSub = "other-sub";
        final String ownSub = ANONYMOUS_ROLE + "-sub";
        final int numMessages = 5;
        superUserAdmin.namespaces().createNamespace(namespace, 1);
        superUserAdmin.topics().createNonPartitionedTopic(topic);
        superUserAdmin.topics().createSubscription(topic, otherSub, MessageId.earliest);
        superUserAdmin.topics().createSubscription(topic, ownSub, MessageId.earliest);
        @Cleanup
        Producer<byte[]> producer = getPulsarService().getClient().newProducer().topic(topic)
                .enableBatching(false)
                .create();
        sendMessages(producer, numMessages);
        superUserAdmin.namespaces().grantPermissionOnNamespace(namespace, ANONYMOUS_ROLE, Set.of(AuthAction.consume));
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);

        // the topic operations reject subscriptions outside the role prefix with a server error
        Assert.assertThrows(PulsarAdminException.class,
                () -> anonymousAdmin.topics().skipAllMessages(topic, otherSub));
        Assert.assertThrows(PulsarAdminException.class,
                () -> anonymousAdmin.topics().resetCursor(topic, otherSub, MessageId.latest));
        Assert.assertThrows(PulsarAdminException.class,
                () -> anonymousAdmin.topics().deleteSubscription(topic, otherSub));
        assertEquals(getMsgBacklog(topic, otherSub), numMessages);

        anonymousAdmin.topics().skipAllMessages(topic, ownSub);
        assertEquals(getMsgBacklog(topic, ownSub), 0);

        // subscription roles are applied
        superUserAdmin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.None);
        superUserAdmin.namespaces().grantPermissionOnSubscription(namespace, otherSub, Set.of("other-role"));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> anonymousAdmin.topics().skipAllMessages(topic, otherSub));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> anonymousAdmin.topics().skipMessages(topic, otherSub, 1));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> anonymousAdmin.topics().resetCursor(topic, otherSub, MessageId.latest));
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                () -> anonymousAdmin.topics().deleteSubscription(topic, otherSub));
        assertEquals(getMsgBacklog(topic, otherSub), numMessages);

        // expiring messages of all subscriptions only expires the subscriptions the role may access
        sendMessages(producer, numMessages);
        Thread.sleep(1500);
        anonymousAdmin.topics().expireMessagesForAllSubscriptions(topic, 1);
        Awaitility.await().untilAsserted(() -> assertEquals(getMsgBacklog(topic, ownSub), 0));
        assertEquals(getMsgBacklog(topic, otherSub), 2 * numMessages);

        producer.close();
        superUserAdmin.namespaces().deleteNamespace(namespace, true);
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
