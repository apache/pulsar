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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Roles, subscription names, namespace properties and dynamic configuration values that the admin client sends as
 * path components reach the broker unchanged: a space and a '+' make two different names, as "team app" and
 * "team+app".
 */
@Test(groups = "broker-admin")
public class AdminApiPathEncodingTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private String newNamespace() throws Exception {
        String namespace = "public/ns-" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace);
        return namespace;
    }

    private String newTopic(String namespace) throws Exception {
        String topic = "persistent://" + namespace + "/" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        return topic;
    }

    @Test
    public void testTopicPermissions() throws Exception {
        String topic = newTopic(newNamespace());
        admin.topics().grantPermission(topic, "team app", Set.of(AuthAction.consume));
        admin.topics().grantPermission(topic, "team+app", Set.of(AuthAction.produce));
        assertThat(admin.topics().getPermissions(topic))
                .containsEntry("team app", Set.of(AuthAction.consume))
                .containsEntry("team+app", Set.of(AuthAction.produce));

        admin.topics().revokePermissions(topic, "team app");
        assertThat(admin.topics().getPermissions(topic))
                .doesNotContainKey("team app")
                .containsEntry("team+app", Set.of(AuthAction.produce));
    }

    @Test
    public void testNamespacePermissions() throws Exception {
        String namespace = newNamespace();
        admin.namespaces().grantPermissionOnNamespace(namespace, "team app", Set.of(AuthAction.consume));
        admin.namespaces().grantPermissionOnNamespace(namespace, "team+app", Set.of(AuthAction.produce));
        assertThat(admin.namespaces().getPermissions(namespace))
                .containsEntry("team app", Set.of(AuthAction.consume))
                .containsEntry("team+app", Set.of(AuthAction.produce));

        admin.namespaces().revokePermissionsOnNamespace(namespace, "team app");
        assertThat(admin.namespaces().getPermissions(namespace))
                .doesNotContainKey("team app")
                .containsEntry("team+app", Set.of(AuthAction.produce));
    }

    @Test
    public void testSubscriptionPermissions() throws Exception {
        String namespace = newNamespace();
        admin.namespaces().grantPermissionOnSubscription(namespace, "team sub", Set.of("team app", "team+app"));
        admin.namespaces().grantPermissionOnSubscription(namespace, "team+sub", Set.of("team app", "team+app"));
        assertThat(admin.namespaces().getPermissionOnSubscription(namespace))
                .containsEntry("team sub", Set.of("team app", "team+app"))
                .containsEntry("team+sub", Set.of("team app", "team+app"));

        admin.namespaces().revokePermissionOnSubscription(namespace, "team sub", "team app");
        assertThat(admin.namespaces().getPermissionOnSubscription(namespace))
                .containsEntry("team sub", Set.of("team+app"))
                .containsEntry("team+sub", Set.of("team app", "team+app"));
    }

    @Test
    public void testNamespaceProperties() throws Exception {
        String namespace = newNamespace();
        admin.namespaces().setProperty(namespace, "team key", "team value");
        admin.namespaces().setProperty(namespace, "team+key", "team+value");
        assertThat(admin.namespaces().getProperties(namespace))
                .containsEntry("team key", "team value")
                .containsEntry("team+key", "team+value");
        assertThat(admin.namespaces().getProperty(namespace, "team key")).isEqualTo("team value");

        assertThat(admin.namespaces().removeProperty(namespace, "team key")).isEqualTo("team value");
        assertThat(admin.namespaces().getProperties(namespace))
                .doesNotContainKey("team key")
                .containsEntry("team+key", "team+value");
    }

    @Test
    public void testUnsubscribeNamespace() throws Exception {
        String namespace = newNamespace();
        String topic = newTopic(namespace);
        admin.topics().createSubscription(topic, "team sub", MessageId.earliest);
        admin.topics().createSubscription(topic, "team+sub", MessageId.earliest);

        admin.namespaces().unsubscribeNamespace(namespace, "team sub");
        assertThat(admin.topics().getSubscriptions(topic)).containsExactly("team+sub");
    }

    @Test
    public void testDynamicConfigurationValue() throws Exception {
        admin.brokers().updateDynamicConfiguration("superUserRoles", "team app,team+app");
        assertThat(admin.brokers().getAllDynamicConfigurations())
                .containsEntry("superUserRoles", "team app,team+app");
        Awaitility.await().untilAsserted(() ->
                assertThat(pulsar.getConfiguration().getSuperUserRoles())
                        .containsExactlyInAnyOrder("team app", "team+app"));
    }

    /** These endpoints read the subscription name with {@code @Encoded} and decode it themselves. */
    @Test
    public void testSubscriptionDispatchRate() throws Exception {
        String topic = newTopic(newNamespace());
        DispatchRate rate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(1024)
                .ratePeriodInSecond(1)
                .build();
        admin.topicPolicies().setSubscriptionDispatchRate(topic, "team sub", rate);
        Awaitility.await().untilAsserted(() ->
                assertThat(admin.topicPolicies().getSubscriptionDispatchRate(topic, "team sub")).isEqualTo(rate));
        assertThat(admin.topicPolicies().getSubscriptionDispatchRate(topic, "team+sub")).isNull();
    }
}
