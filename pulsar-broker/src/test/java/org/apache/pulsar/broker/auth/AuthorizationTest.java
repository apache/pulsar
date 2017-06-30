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

import java.util.EnumSet;

import org.apache.pulsar.broker.authorization.AuthorizationManager;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.broker.authorization.AuthorizationManager;
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
        internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    void simple() throws Exception {
        AuthorizationManager auth = pulsar.getBrokerService().getAuthorizationManager();

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my-role"), false);

        admin.clusters().createCluster("c1", new ClusterData());
        admin.properties().createProperty("p1", new PropertyAdmin(Lists.newArrayList("role1"), Sets.newHashSet("c1")));
        waitForChange();
        admin.namespaces().createNamespace("p1/c1/ns1");
        waitForChange();

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my-role"), false);

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my-role", EnumSet.of(AuthAction.produce));
        waitForChange();

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my-role"), true);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my-role"), true);

        admin.persistentTopics().grantPermission("persistent://p1/c1/ns1/ds2", "other-role",
                EnumSet.of(AuthAction.consume));
        waitForChange();

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "other-role"), true);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my-role"), true);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds2"), "other-role"), false);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds2"), "other-role"), true);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds2"), "no-access-role"), false);

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "no-access-role"), false);

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my-role", EnumSet.allOf(AuthAction.class));
        waitForChange();

        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my-role"), true);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my-role"), true);

        // test for wildcard

        // namespace prefix match
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.2"), false);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), false);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "other.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "other.role.2"), false);

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my.role.*", EnumSet.of(AuthAction.produce));
        waitForChange();

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), true);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.2"), true);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), true);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "other.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "other.role.2"), false);

        // namespace suffix match
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.my"), false);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), false);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.other"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.other"), false);

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "*.role.my", EnumSet.of(AuthAction.consume));
        waitForChange();

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), true);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.my"), true);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), false);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), true);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.other"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.other"), false);

        // revoke for next test
        admin.namespaces().revokePermissionsOnNamespace("p1/c1/ns1", "my.role.*");
        admin.namespaces().revokePermissionsOnNamespace("p1/c1/ns1", "*.role.my");
        waitForChange();

        // destination prefix match
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.2"), false);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), false);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "other.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "other.role.2"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "my.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "my.role.2"), false);

        admin.persistentTopics().grantPermission("persistent://p1/c1/ns1/ds1", "my.*",
                EnumSet.of(AuthAction.produce));
        waitForChange();

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), true);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.2"), true);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), true);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "my.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "other.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "other.role.2"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "my.role.1"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "my.role.2"), false);

        // destination suffix match
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.my"), false);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), false);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.other"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.other"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "1.role.my"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "2.role.my"), false);

        admin.persistentTopics().grantPermission("persistent://p1/c1/ns1/ds1", "*.my",
                EnumSet.of(AuthAction.consume));
        waitForChange();

        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), true);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.my"), true);
        assertEquals(auth.canProduce(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), false);
        assertEquals(auth.canConsume(DestinationName.get("persistent://p1/c1/ns1/ds1"), "1.role.my"), true);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.other"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds1"), "2.role.other"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "1.role.my"), false);
        assertEquals(auth.canLookup(DestinationName.get("persistent://p1/c1/ns1/ds2"), "2.role.my"), false);

        admin.namespaces().deleteNamespace("p1/c1/ns1");
        admin.properties().deleteProperty("p1");
        admin.clusters().deleteCluster("c1");
    }

    private static void waitForChange() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
        }
    }
}
