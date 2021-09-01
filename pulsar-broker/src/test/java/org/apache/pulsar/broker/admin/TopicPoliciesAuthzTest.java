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
package org.apache.pulsar.broker.admin;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockAuthentication;
import org.apache.pulsar.broker.auth.MockAuthenticationProvider;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.*;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Topic Policies are only meant to be modified by tenant admin roles. This
 * test class verifies each tenant policy operation's authorization implementation
 * by ensuring an authenticated user with tenant admin privileges can make these calls
 * without exception and an authenticated user without tenant admin privileges fails
 * to make these calls with a resulting 403 response code.
 */
@Slf4j
@Test(groups = "broker")
public class TopicPoliciesAuthzTest extends MockedPulsarServiceBaseTest {

    private final String clusterName = "test";
    private final String testTenant = "my-tenant";
    private final String testNamespace = "my-namespace";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/my-topic";
    // Create the roles that will be used to verify the different levels of permission
    private final String superuserRole = "pass.superuser";
    private final String tenantAdminRole = "pass.tenantadmin";
    private final String basicUserRole = "pass.basic";
    private PulsarAdmin tenantAdmin = null;
    private PulsarAdmin basicUser = null;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setSuperUserRoles(ImmutableSet.of(superuserRole));
        conf.setAuthenticationProviders(ImmutableSet.of(MockAuthenticationProvider.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(MockAuthentication.class.getName());
        conf.setBrokerClientAuthenticationParameters("user:" + superuserRole);

        // Now that the config is set, start the cluster
        super.internalSetup();

        admin.clusters().createCluster(clusterName,
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet(tenantAdminRole), Sets.newHashSet(clusterName));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        // User has all basic permissions for the namespace, but is lacking admin privileges for tenant
        admin.namespaces().grantPermissionOnNamespace(myNamespace, basicUserRole,
                Sets.newHashSet(AuthAction.values()));
        admin.topics().createPartitionedTopic(testTopic, 2);
        // Create the admin clients we'll test with
        tenantAdmin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(new MockAuthentication(tenantAdminRole)).build();
        basicUser = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(new MockAuthentication(basicUserRole)).build();
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {
        pulsarAdminBuilder.authentication(new MockAuthentication(superuserRole));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Runnables that, when run, should result in PulsarAdminExceptions that have 403 status codes.
     * @param runnable - a runnable that is expected to throw an exception
     */
    private void assertFailsWith403(Assert.ThrowingRunnable runnable) {
        try {
            runnable.run();
            Assert.fail("Succeeded on call that was supposed to fail.");
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.FORBIDDEN_403);
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable e) {
            Assert.fail("Received unexpected exception", e);
        }
    }

    @Test
    public void testOffloadPoliciesAuthz() throws Exception {
        OffloadPolicies policies = OffloadPolicies.builder().build();
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setOffloadPolicies(testTopic, policies);
        tenantAdmin.topics().getOffloadPolicies(testTopic);
        tenantAdmin.topics().removeOffloadPolicies(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getOffloadPolicies(testTopic));
        assertFailsWith403(() -> basicUser.topics().setOffloadPolicies(testTopic, policies));
        assertFailsWith403(() -> basicUser.topics().removeOffloadPolicies(testTopic));
    }

    @Test
    public void testMaxUnackedMessagesOnConsumerAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setMaxUnackedMessagesOnConsumer(testTopic, 1);
        tenantAdmin.topics().getMaxUnackedMessagesOnConsumer(testTopic);
        tenantAdmin.topics().removeMaxUnackedMessagesOnConsumer(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getMaxUnackedMessagesOnConsumer(testTopic));
        assertFailsWith403(() -> basicUser.topics().setMaxUnackedMessagesOnConsumer(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeMaxUnackedMessagesOnConsumer(testTopic));
    }

    @Test
    public void testDeduplicationSnapshotIntervalAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setDeduplicationSnapshotInterval(testTopic, 1);
        tenantAdmin.topics().getDeduplicationSnapshotInterval(testTopic);
        tenantAdmin.topics().removeDeduplicationSnapshotInterval(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getDeduplicationSnapshotInterval(testTopic));
        assertFailsWith403(() -> basicUser.topics().setDeduplicationSnapshotInterval(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeDeduplicationSnapshotInterval(testTopic));
    }

    @Test
    public void testInactiveTopicPoliciesAuthz() throws Exception {
        InactiveTopicPolicies policies = new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,
                60, true);
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setInactiveTopicPolicies(testTopic, policies);
        tenantAdmin.topics().getInactiveTopicPolicies(testTopic);
        tenantAdmin.topics().removeInactiveTopicPolicies(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getInactiveTopicPolicies(testTopic));
        assertFailsWith403(() -> basicUser.topics().setInactiveTopicPolicies(testTopic, policies));
        assertFailsWith403(() -> basicUser.topics().removeInactiveTopicPolicies(testTopic));
    }

    @Test
    public void testMaxUnackedMessagesOnSubscriptionAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setMaxUnackedMessagesOnSubscription(testTopic, 1);
        tenantAdmin.topics().getMaxUnackedMessagesOnSubscription(testTopic);
        tenantAdmin.topics().removeMaxUnackedMessagesOnSubscription(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getMaxUnackedMessagesOnSubscription(testTopic));
        assertFailsWith403(() -> basicUser.topics().setMaxUnackedMessagesOnSubscription(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeMaxUnackedMessagesOnSubscription(testTopic));
    }

    @Test
    public void testDelayedDeliveryPolicyAuthz() throws Exception {
        DelayedDeliveryPolicies policies = DelayedDeliveryPolicies.builder().build();
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setDelayedDeliveryPolicy(testTopic, policies);
        tenantAdmin.topics().getDelayedDeliveryPolicy(testTopic);
        tenantAdmin.topics().removeDelayedDeliveryPolicy(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getDelayedDeliveryPolicy(testTopic));
        assertFailsWith403(() -> basicUser.topics().setDelayedDeliveryPolicy(testTopic, policies));
        assertFailsWith403(() -> basicUser.topics().removeDelayedDeliveryPolicy(testTopic));
    }

    @Test
    public void testBacklogQuotaAuthz() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder().build();
        BacklogQuota.BacklogQuotaType backlogQuotaType = BacklogQuota.BacklogQuotaType.destination_storage;
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setBacklogQuota(testTopic, backlogQuota, backlogQuotaType);
        tenantAdmin.topics().getBacklogQuotaMap(testTopic).get(backlogQuotaType);
        tenantAdmin.topics().removeBacklogQuota(testTopic, backlogQuotaType);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getBacklogQuotaMap(testTopic));
        assertFailsWith403(() -> basicUser.topics().setBacklogQuota(testTopic, backlogQuota, backlogQuotaType));
        assertFailsWith403(() -> basicUser.topics().removeBacklogQuota(testTopic, backlogQuotaType));
    }

    @Test
    public void testMessageTTLAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setMessageTTL(testTopic, 1);
        tenantAdmin.topics().getMessageTTL(testTopic);
        tenantAdmin.topics().removeMessageTTL(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getMessageTTL(testTopic));
        assertFailsWith403(() -> basicUser.topics().setMessageTTL(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeMessageTTL(testTopic));
    }

    @Test
    public void testDeduplicationStatusAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setDeduplicationStatus(testTopic, true);
        tenantAdmin.topics().getDeduplicationStatus(testTopic);
        tenantAdmin.topics().removeDeduplicationStatus(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getDeduplicationStatus(testTopic));
        assertFailsWith403(() -> basicUser.topics().setDeduplicationStatus(testTopic, true));
        assertFailsWith403(() -> basicUser.topics().removeDeduplicationStatus(testTopic));
    }

    @Test
    public void testRetentionAuthz() throws Exception {
        RetentionPolicies policies = new RetentionPolicies(1, 1);
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setRetention(testTopic, policies);
        tenantAdmin.topics().getRetention(testTopic);
        tenantAdmin.topics().removeRetention(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getRetention(testTopic));
        assertFailsWith403(() -> basicUser.topics().setRetention(testTopic, policies));
        assertFailsWith403(() -> basicUser.topics().removeRetention(testTopic));
    }

    @Test
    public void testPersistenceAuthz() throws Exception {
        PersistencePolicies policies = new PersistencePolicies(3, 2, 1, 0);
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setPersistence(testTopic, policies);
        tenantAdmin.topics().getPersistence(testTopic);
        tenantAdmin.topics().removePersistence(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getPersistence(testTopic));
        assertFailsWith403(() -> basicUser.topics().setPersistence(testTopic, policies));
        assertFailsWith403(() -> basicUser.topics().removePersistence(testTopic));
    }

    @Test
    public void testMaxSubscriptionsPerTopicAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setMaxSubscriptionsPerTopic(testTopic, 1);
        tenantAdmin.topics().getMaxSubscriptionsPerTopic(testTopic);
        tenantAdmin.topics().removeMaxSubscriptionsPerTopic(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getMaxSubscriptionsPerTopic(testTopic));
        assertFailsWith403(() -> basicUser.topics().setMaxSubscriptionsPerTopic(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeMaxSubscriptionsPerTopic(testTopic));
    }

    @Test
    public void testReplicatorDispatchRateAuthz() throws Exception {
        DispatchRate rate = DispatchRate.builder().build();
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setReplicatorDispatchRate(testTopic, rate);
        tenantAdmin.topics().getReplicatorDispatchRate(testTopic);
        tenantAdmin.topics().removeReplicatorDispatchRate(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getReplicatorDispatchRate(testTopic));
        assertFailsWith403(() -> basicUser.topics().setReplicatorDispatchRate(testTopic, rate));
        assertFailsWith403(() -> basicUser.topics().removeReplicatorDispatchRate(testTopic));
    }

    @Test
    public void testMaxProducersAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setMaxProducers(testTopic, 1);
        tenantAdmin.topics().getMaxProducers(testTopic);
        tenantAdmin.topics().removeMaxProducers(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getMaxProducers(testTopic));
        assertFailsWith403(() -> basicUser.topics().setMaxProducers(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeMaxProducers(testTopic));
    }

    @Test
    public void testMaxConsumersAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setMaxConsumers(testTopic, 1);
        tenantAdmin.topics().getMaxConsumers(testTopic);
        tenantAdmin.topics().removeMaxConsumers(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getMaxConsumers(testTopic));
        assertFailsWith403(() -> basicUser.topics().setMaxConsumers(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeMaxConsumers(testTopic));
    }

    @Test
    public void testMaxMessageSizeAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setMaxMessageSize(testTopic, 1);
        tenantAdmin.topics().getMaxMessageSize(testTopic);
        tenantAdmin.topics().removeMaxMessageSize(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getMaxMessageSize(testTopic));
        assertFailsWith403(() -> basicUser.topics().setMaxMessageSize(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeMaxMessageSize(testTopic));
    }

    @Test
    public void testDispatchRateAuthz() throws Exception {
        DispatchRate rate = DispatchRate.builder().build();
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setDispatchRate(testTopic, rate);
        tenantAdmin.topics().getDispatchRate(testTopic);
        tenantAdmin.topics().removeDispatchRate(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getDispatchRate(testTopic));
        assertFailsWith403(() -> basicUser.topics().setDispatchRate(testTopic, rate));
        assertFailsWith403(() -> basicUser.topics().removeDispatchRate(testTopic));
    }

    @Test
    public void testSubscriptionDispatchRateAuthz() throws Exception {
        DispatchRate rate = DispatchRate.builder().build();
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setSubscriptionDispatchRate(testTopic, rate);
        tenantAdmin.topics().getSubscriptionDispatchRate(testTopic);
        tenantAdmin.topics().removeSubscriptionDispatchRate(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getSubscriptionDispatchRate(testTopic));
        assertFailsWith403(() -> basicUser.topics().setSubscriptionDispatchRate(testTopic, rate));
        assertFailsWith403(() -> basicUser.topics().removeSubscriptionDispatchRate(testTopic));
    }

    @Test
    public void testCompactionThresholdAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setCompactionThreshold(testTopic, 1);
        tenantAdmin.topics().getCompactionThreshold(testTopic);
        tenantAdmin.topics().removeCompactionThreshold(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getCompactionThreshold(testTopic));
        assertFailsWith403(() -> basicUser.topics().setCompactionThreshold(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeCompactionThreshold(testTopic));
    }

    @Test
    public void testMaxConsumersPerSubscriptionAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setMaxConsumersPerSubscription(testTopic, 1);
        tenantAdmin.topics().getMaxConsumersPerSubscription(testTopic);
        tenantAdmin.topics().removeMaxConsumersPerSubscription(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getMaxConsumersPerSubscription(testTopic));
        assertFailsWith403(() -> basicUser.topics().setMaxConsumersPerSubscription(testTopic, 1));
        assertFailsWith403(() -> basicUser.topics().removeMaxConsumersPerSubscription(testTopic));
    }

    @Test
    public void testPublishRateAuthz() throws Exception {
        PublishRate rate = new PublishRate(1, 1);
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setPublishRate(testTopic, rate);
        tenantAdmin.topics().getPublishRate(testTopic);
        tenantAdmin.topics().removePublishRate(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getPublishRate(testTopic));
        assertFailsWith403(() -> basicUser.topics().setPublishRate(testTopic, rate));
        assertFailsWith403(() -> basicUser.topics().removePublishRate(testTopic));
    }

    @Test
    public void testSubscriptionTypesEnabledAuthz() throws Exception {
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setSubscriptionTypesEnabled(testTopic, Sets.newHashSet());
        tenantAdmin.topics().getSubscriptionTypesEnabled(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getSubscriptionTypesEnabled(testTopic));
        assertFailsWith403(() -> basicUser.topics().setSubscriptionTypesEnabled(testTopic, Sets.newHashSet()));
    }

    @Test
    public void testSubscribeRateAuthz() throws Exception {
        SubscribeRate rate = new SubscribeRate(1, 1);
        // Perform all ops. Don't worry about resulting values, as this class is _only_ testing permission
        tenantAdmin.topics().setSubscribeRate(testTopic, rate);
        tenantAdmin.topics().getSubscribeRate(testTopic);
        tenantAdmin.topics().removeSubscribeRate(testTopic);

        // Perform all ops and expect all to fail.
        assertFailsWith403(() -> basicUser.topics().getSubscribeRate(testTopic));
        assertFailsWith403(() -> basicUser.topics().setSubscribeRate(testTopic, rate));
        assertFailsWith403(() -> basicUser.topics().removeSubscribeRate(testTopic));
    }
}
