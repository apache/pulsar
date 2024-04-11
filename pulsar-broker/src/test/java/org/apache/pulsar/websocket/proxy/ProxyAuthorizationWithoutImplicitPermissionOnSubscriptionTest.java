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
package org.apache.pulsar.websocket.proxy;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Class that initializes the WebSocketService disabling {@link WebSocketProxyConfiguration#setGrantImplicitPermissionOnSubscription(boolean)}.
 * We must have this class on its own because the WebSocketProxyConfiguration is converted to the ServiceConfiguration
 * on start up, so it is not a dynamic property that we can change after the service has started.
 */

@Test(groups = "websocket")
public class ProxyAuthorizationWithoutImplicitPermissionOnSubscriptionTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeTest.class);
    private WebSocketService service;
    private final String configClusterName = "c1";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setClusterName(configClusterName);
        internalSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        Set<String> superUser = Sets.newHashSet("");
        config.setAuthorizationEnabled(true);
        config.setSuperUserRoles(superUser);
        config.setClusterName("c1");
        config.setWebServicePort(Optional.of(0));
        config.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        config.setGrantImplicitPermissionOnSubscription(false);
        service = spyWithClassAndConstructorArgs(WebSocketService.class, config);
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(service)
                .createConfigMetadataStore(anyString(), anyInt(), anyBoolean());
        service.start();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (service != null) {
            service.close();
        }
        log.info("Finished Cleaning Up Test setup");
    }


    @Test
    public void testAuthorizationServiceDirectly() throws Exception {
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

        // Expect false because we disabled the implicit permission on subscription
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "other-role", null, "sub"));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "no-access-role", null,"sub"));

        // Grant permission
        admin.namespaces().grantPermissionOnSubscription("p1/c1/ns1", "sub", Set.of("other-role"));

        // Expect only true for "other-role" because we granted permission for only that one
        assertTrue(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "other-role", null, "sub"));
        assertFalse(auth.canConsume(TopicName.get("persistent://p1/c1/ns1/ds2"), "no-access-role", null,"sub"));


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
