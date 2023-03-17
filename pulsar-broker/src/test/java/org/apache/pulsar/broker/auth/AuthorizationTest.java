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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.EnumSet;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

    @Test
    public void testOriginalRoleValidation() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProxyRoles(Collections.singleton("proxy"));
        AuthorizationService auth = new AuthorizationService(conf, Mockito.mock(PulsarResources.class));

        // Original principal should be supplied when authenticatedPrincipal is proxy role
        assertTrue(auth.isValidOriginalPrincipal("proxy", "client", (SocketAddress) null, false));

        // Non proxy role should not supply originalPrincipal
        assertTrue(auth.isValidOriginalPrincipal("client", "", (SocketAddress) null, false));
        assertTrue(auth.isValidOriginalPrincipal("client", null, (SocketAddress) null, false));

        // Edge cases that differ because binary protocol and http protocol have different expectations
        assertTrue(auth.isValidOriginalPrincipal("client", "client", (SocketAddress) null, true));
        // This assert flips to assertFalse in the 3.0 release.
        assertTrue(auth.isValidOriginalPrincipal("client", "client", (SocketAddress) null, false));

        // Only likely in cases when authentication is disabled, but we still define these to be valid.
        assertTrue(auth.isValidOriginalPrincipal(null, null, (SocketAddress) null, false));
        assertTrue(auth.isValidOriginalPrincipal(null, "", (SocketAddress) null, false));
        assertTrue(auth.isValidOriginalPrincipal("", null, (SocketAddress) null, false));
        assertTrue(auth.isValidOriginalPrincipal("", "", (SocketAddress) null, false));

        // Proxy role must supply an original principal
        assertFalse(auth.isValidOriginalPrincipal("proxy", "", (SocketAddress) null, false));
        assertFalse(auth.isValidOriginalPrincipal("proxy", null, (SocketAddress) null, false));

        // OriginalPrincipal cannot be proxy role
        assertFalse(auth.isValidOriginalPrincipal("proxy", "proxy", (SocketAddress) null, false));
        // The next 3 asserts flip to assertFalse in the 3.0 release.
        assertTrue(auth.isValidOriginalPrincipal("client", "proxy", (SocketAddress) null, false));
        assertTrue(auth.isValidOriginalPrincipal("", "proxy", (SocketAddress) null, false));
        assertTrue(auth.isValidOriginalPrincipal(null, "proxy", (SocketAddress) null, false));

        // Must gracefully handle a missing AuthenticationDataSource
        assertTrue(auth.isValidOriginalPrincipal("proxy", "client", (AuthenticationDataSource) null));
    }

    @Test
    public void testGetListWithGetBundleOp() throws Exception {
        String tenant = "p1";
        String namespaceV1 = "p1/global/ns1";
        String namespaceV2 = "p1/ns2";
        admin.clusters().createCluster("c1", ClusterData.builder().build());
        admin.tenants().createTenant(tenant, new TenantInfoImpl(Sets.newHashSet("role1"), Sets.newHashSet("c1")));
        admin.namespaces().createNamespace(namespaceV1, Sets.newHashSet("c1"));
        admin.namespaces().grantPermissionOnNamespace(namespaceV1, "pass.pass2", EnumSet.of(AuthAction.produce));
        admin.namespaces().createNamespace(namespaceV2, Sets.newHashSet("c1"));
        admin.namespaces().grantPermissionOnNamespace(namespaceV2, "pass.pass2", EnumSet.of(AuthAction.produce));
        PulsarAdmin admin2 = PulsarAdmin.builder().serviceHttpUrl(brokerUrl != null
                        ? brokerUrl.toString()
                        : brokerUrlTls.toString())
                .authentication(new MockAuthentication("pass.pass2"))
                .build();
        when(pulsar.getAdminClient()).thenReturn(admin2);
        Assert.assertEquals(admin2.topics().getList(namespaceV1, TopicDomain.non_persistent).size(), 0);
        Assert.assertEquals(admin2.topics().getList(namespaceV2, TopicDomain.non_persistent).size(), 0);
    }

    private static void waitForChange() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
    }
}
