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
package org.apache.pulsar.websocket.proxy;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

import java.util.EnumSet;
import java.util.Set;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authorization.AuthorizationManager;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ProxyAuthorizationTest extends MockedPulsarServiceBaseTest {
    private WebSocketService service;
    private static final int TEST_PORT = PortManager.nextFreePort();;

    public ProxyAuthorizationTest() {
        super();
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setClusterName("c1");
        internalSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        Set<String> superUser = Sets.newHashSet("");
        config.setAuthorizationEnabled(true);
        config.setGlobalZookeeperServers("dummy-zk-servers");
        config.setSuperUserRoles(superUser);
        config.setClusterName("c1");
        config.setWebServicePort(TEST_PORT);
        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        service.start();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
        service.close();
    }

    @Test
    public void test() throws Exception {
        AuthorizationManager auth = service.getAuthorizationManager();

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
