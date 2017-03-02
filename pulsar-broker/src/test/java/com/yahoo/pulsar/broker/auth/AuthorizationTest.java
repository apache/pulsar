/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.auth;

import static org.testng.Assert.assertEquals;

import java.util.EnumSet;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.policies.data.AuthAction;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.PropertyAdmin;
import com.yahoo.pulsar.broker.authorization.AuthorizationManager;
import com.yahoo.pulsar.client.admin.PulsarAdminException;

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
