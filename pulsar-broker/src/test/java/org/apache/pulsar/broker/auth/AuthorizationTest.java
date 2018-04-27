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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.EnumSet;

import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Test
public class AuthorizationTest extends MockedPulsarServiceBaseTest {

    public AuthorizationTest() {
        super();
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setClusterName("c1");
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet("pulsar.super_user"));
        internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    void simple() throws Exception {
        AuthorizationService auth = pulsar.getBrokerService().getAuthorizationService();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null), false);

        admin.clusters().createCluster("c1", new ClusterData());
        admin.tenants().createTenant("p1", new TenantInfo(Sets.newHashSet("role1"), Sets.newHashSet("c1")));
        waitForChange();
        admin.namespaces().createNamespace("p1/c1/ns1");
        waitForChange();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null), false);

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my-role", EnumSet.of(AuthAction.produce));
        waitForChange();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null), true);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null), true);

        admin.topics().grantPermission("persistent://p1/c1/ns1/ds2", "other-role",
                EnumSet.of(AuthAction.consume));
        waitForChange();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "other-role", null), true);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null), true);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds2"), "other-role", null), false);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "other-role", null, null), true);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "no-access-role", null, null), false);

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "no-access-role", null), false);

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my-role", EnumSet.allOf(AuthAction.class));
        waitForChange();

        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null), true);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null, null), true);

        // test for wildcard

        // namespace prefix match
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.2", null), false);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null), false);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null, null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.1", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.2", null), false);

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my.role.*", EnumSet.of(AuthAction.produce));
        waitForChange();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null), true);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.2", null), true);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null), true);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null, null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.1", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.2", null), false);

        // namespace suffix match
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.my", null), false);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null), false);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null, null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null), false);

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "*.role.my", EnumSet.of(AuthAction.consume));
        waitForChange();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null), true);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.my", null), true);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null), false);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null, null), true);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null), false);

        // revoke for next test
        admin.namespaces().revokePermissionsOnNamespace("p1/c1/ns1", "my.role.*");
        admin.namespaces().revokePermissionsOnNamespace("p1/c1/ns1", "*.role.my");
        waitForChange();

        // topic prefix match
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.2", null), false);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null), false);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null, null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.1", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.2", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "my.role.1", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "my.role.2", null), false);

        admin.topics().grantPermission("persistent://p1/c1/ns1/ds1", "my.*",
                EnumSet.of(AuthAction.produce));
        waitForChange();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null), true);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.2", null), true);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null), true);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my.role.1", null, null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.1", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "other.role.2", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "my.role.1", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "my.role.2", null), false);

        // topic suffix match
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.my", null), false);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null), false);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null, null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "1.role.my", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "2.role.my", null), false);

        admin.topics().grantPermission("persistent://p1/c1/ns1/ds1", "*.my",
                EnumSet.of(AuthAction.consume));
        waitForChange();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null), true);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.my", null), true);
        assertEquals(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null), false);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "1.role.my", null, null), true);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "2.role.other", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "1.role.my", null), false);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds2"), "2.role.my", null), false);

        admin.topics().revokePermissions("persistent://p1/c1/ns1/ds1", "my.*");
        admin.topics().revokePermissions("persistent://p1/c1/ns1/ds1", "*.my");

        // tests for subscription auth mode
        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "*", EnumSet.of(AuthAction.consume));
        admin.namespaces().setSubscriptionAuthMode("p1/c1/ns1", SubscriptionAuthMode.Prefix);
        waitForChange();

        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "role1", null), true);
        assertEquals(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "role2", null), true);
        try {
            assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "role1", null, "sub1"), false);
            fail();
        } catch (Exception e) {}
        try {
            assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "role2", null, "sub2"), false);
            fail();
        } catch (Exception e) {}

        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "role1", null, "role1-sub1"), true);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "role2", null, "role2-sub2"), true);
        assertEquals(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "pulsar.super_user", null, "role3-sub1"), true);

        admin.namespaces().deleteNamespace("p1/c1/ns1");
        admin.tenants().deleteTenant("p1");
        admin.clusters().deleteCluster("c1");
    }

    private static void waitForChange() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
        }
    }
}
