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
package org.apache.pulsar.broker.auth;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.EnumSet;

import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

@Test(groups = "flaky")
public class AuthorizationTest extends MockedPulsarServiceBaseTest {

    public AuthorizationTest() {
        super();
    }

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setClusterName("c1");
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(
                Sets.newHashSet("org.apache.pulsar.broker.auth.MockAuthenticationProvider"));
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet("pulsar.super_user", "pass.pass"));
        internalSetup();
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {
        pulsarAdminBuilder.authentication(new MockAuthentication("pass.pass"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void simple() throws Exception {
        AuthorizationService auth = pulsar.getBrokerService().getAuthorizationService();

        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null));

        admin.clusters().createCluster("c1", ClusterData.builder().build());
        admin.tenants().createTenant("p1", new TenantInfoImpl(Sets.newHashSet("role1"), Sets.newHashSet("c1")));
        waitForChange();
        admin.namespaces().createNamespace("p1/c1/ns1");
        waitForChange();

        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null));

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my-role", EnumSet.of(AuthAction.produce));
        waitForChange();

        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null));
        assertTrue(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null));

        admin.topics().grantPermission("persistent://p1/c1/ns1/ds2", "other-role",
                EnumSet.of(AuthAction.consume));
        waitForChange();

        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "other-role", null));
        assertTrue(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null));
        assertFalse(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds2"), "other-role", null));
        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "other-role", null, null));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "no-access-role", null, null));

        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "no-access-role", null));

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my-role", EnumSet.allOf(AuthAction.class));
        waitForChange();

        assertTrue(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null));
        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null, null));

        // test for wildcard

        // namespace prefix match
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.2", null));
        assertFalse(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null, null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.1", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.2", null));

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my.role.*", EnumSet.of(AuthAction.produce));
        waitForChange();

        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null));
        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.2", null));
        assertTrue(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null, null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.1", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.2", null));

        // namespace suffix match
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.my", null));
        assertFalse(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null, null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null));

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "*.role.my", EnumSet.of(AuthAction.consume));
        waitForChange();

        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null));
        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.my", null));
        assertFalse(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null));
        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null, null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null));

        // revoke for next test
        admin.namespaces().revokePermissionsOnNamespace("p1/c1/ns1", "my.role.*");
        admin.namespaces().revokePermissionsOnNamespace("p1/c1/ns1", "*.role.my");
        waitForChange();

        // topic prefix match
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.2", null));
        assertFalse(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null, null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.1", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.2", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "my.role.1", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "my.role.2", null));

        admin.topics().grantPermission("persistent://p1/c1/ns1/ds1", "my.*",
                EnumSet.of(AuthAction.produce));
        waitForChange();

        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null));
        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.2", null));
        assertTrue(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null, null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.1", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.2", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "my.role.1", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "my.role.2", null));

        // topic suffix match
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.my", null));
        assertFalse(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null, null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "1.role.my", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "2.role.my", null));

        admin.topics().grantPermission("persistent://p1/c1/ns1/ds1", "*.my",
                EnumSet.of(AuthAction.consume));
        waitForChange();

        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null));
        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.my", null));
        assertFalse(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null));
        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null, null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "1.role.my", null));
        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "2.role.my", null));

        admin.topics().revokePermissions("persistent://p1/c1/ns1/ds1", "my.*");
        admin.topics().revokePermissions("persistent://p1/c1/ns1/ds1", "*.my");

        // tests for subscription auth mode
        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "*", EnumSet.of(AuthAction.consume));
        admin.namespaces().setSubscriptionAuthMode("p1/c1/ns1", SubscriptionAuthMode.None);
        Assert.assertEquals(admin.namespaces().getSubscriptionAuthMode("p1/c1/ns1"),
                SubscriptionAuthMode.None);
        admin.namespaces().setSubscriptionAuthMode("p1/c1/ns1", SubscriptionAuthMode.Prefix);
        Assert.assertEquals(admin.namespaces().getSubscriptionAuthMode("p1/c1/ns1"),
                SubscriptionAuthMode.Prefix);
        waitForChange();

        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "role1", null));
        assertTrue(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "role2", null));
        try {
            assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "role1", null, "sub1"));
            fail();
        } catch (Exception ignored) {}
        try {
            assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "role2", null, "sub2"));
            fail();
        } catch (Exception ignored) {}

        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "role1", null, "role1-sub1"));
        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "role2", null, "role2-sub2"));
        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "pulsar.super_user", null, "role3-sub1"));

        admin.namespaces().deleteNamespace("p1/c1/ns1");
        admin.tenants().deleteTenant("p1");
        admin.clusters().deleteCluster("c1");
    }

    private static void waitForChange() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
    }
}
