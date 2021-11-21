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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "websocket")
public class ProxyAuthorizationTest extends MockedPulsarServiceBaseTest {
    private WebSocketService service;
    private final String configClusterName = "c1";

    public ProxyAuthorizationTest() {
        super();
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setClusterName(configClusterName);
        internalSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        Set<String> superUser = Sets.newHashSet("");
        config.setAuthorizationEnabled(true);
        config.setConfigurationStoreServers("dummy-zk-servers");
        config.setSuperUserRoles(superUser);
        config.setClusterName("c1");
        config.setWebServicePort(Optional.of(0));
        config.setConfigurationStoreServers(GLOBAL_DUMMY_VALUE);
        service = spy(new WebSocketService(config));
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(service).createMetadataStore(anyString(), anyInt());
        service.start();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
        if (service != null) {
            service.close();
        }
    }

    @Test
    public void test() throws Exception {
        AuthorizationService auth = service.getAuthorizationService();

        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null));

        admin.clusters().createCluster(configClusterName, ClusterData.builder().build());
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
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "no-access-role", null,null));

        assertFalse(auth.canLookup(TopicName.get("persistent://p1/c1/ns1/ds1"), "no-access-role", null));

        admin.namespaces().grantPermissionOnNamespace("p1/c1/ns1", "my-role", EnumSet.allOf(AuthAction.class));
        waitForChange();

        assertTrue(auth.canProduce(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null));
        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds1"), "my-role", null, null));

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
