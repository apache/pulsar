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

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.admin.v2.PersistentTopics;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.UriInfo;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class AdminApiGetLastMessageIdTest extends MockedPulsarServiceBaseTest {

    private PersistentTopics persistentTopics;
    private final String testTenant = "my-tenant";
    private final String testLocalCluster = "use";
    private final String testNamespace = "my-namespace";
    protected Field uriField;
    protected UriInfo uriInfo;

    @BeforeClass
    public void initPersistentTopics() throws Exception {
        uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriInfo = mock(UriInfo.class);
    }

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant("prop",
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop/ns-abc");
        admin.namespaces().setNamespaceReplicationClusters("prop/ns-abc", Sets.newHashSet("test"));
        persistentTopics = spy(new PersistentTopics());
        persistentTopics.setServletContext(new MockServletContext());
        persistentTopics.setPulsar(pulsar);

        doReturn(mockZookKeeper).when(persistentTopics).globalZk();
        doReturn(mockZookKeeper).when(persistentTopics).localZk();
        doReturn(pulsar.getConfigurationCache().propertiesCache()).when(persistentTopics).tenantsCache();
        doReturn(pulsar.getConfigurationCache().policiesCache()).when(persistentTopics).policiesCache();
        doReturn(false).when(persistentTopics).isRequestHttps();
        doReturn(null).when(persistentTopics).originalPrincipal();
        doReturn("test").when(persistentTopics).clientAppId();
        doReturn("persistent").when(persistentTopics).domain();
        doNothing().when(persistentTopics).validateAdminAccessForTenant(this.testTenant);
        doReturn(mock(AuthenticationDataHttps.class)).when(persistentTopics).clientAuthData();
    }

    @Override
    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetLastMessageId() throws Exception {
        try {
            persistentTopics.getLastMessageId(testTenant, testNamespace, "my-topic", true);
        } catch (Exception e) {
            //System.out.println(e.getMessage());
            Assert.assertEquals("Topic not found", e.getMessage());
        }

        String key = "legendtkl";
        final String topicName = "persistent://prop/ns-abc/my-topic";
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        // 3. Publish message and get message id
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        MessageId id = persistentTopics.getLastMessageId("prop", "ns-abc", "my-topic", true);
        System.out.println(id.toString());
        Assert.assertTrue(((MessageIdImpl)id).getLedgerId() >= 0);
        Assert.assertEquals(numberOfMessages-1, ((MessageIdImpl)id).getEntryId());

        // send more numberOfMessages messages, the last message id should be numberOfMessages*2-1
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }
        id = persistentTopics.getLastMessageId("prop", "ns-abc", "my-topic", true);
        System.out.println(id.toString());
        Assert.assertTrue(((MessageIdImpl)id).getLedgerId() > 0);
        Assert.assertEquals( 2 * numberOfMessages -1, ((MessageIdImpl)id).getEntryId());

        System.out.println(id.toString());
    }
}
