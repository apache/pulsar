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
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.MessageId;
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
import java.util.List;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class PersistentTopicsTest extends MockedPulsarServiceBaseTest {

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
        admin.clusters().createCluster("use", new ClusterData("http://broker-use.com:" + BROKER_WEBSERVICE_PORT));
        admin.tenants().createTenant(this.testTenant,
                new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet(testLocalCluster, "test")));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet(testLocalCluster, "test"));
    }

    @Override
    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetSubscriptions() {
        String testLocalTopicName = "topic-not-found";
        try {
            persistentTopics.getSubscriptions(testTenant, testNamespace, testLocalTopicName, true);
        } catch (Exception e) {
            Assert.assertEquals("Topic not found", e.getMessage());
        }
        try {
            persistentTopics.getSubscriptions(testTenant, testNamespace, testLocalTopicName + "-partition-0", true);
        } catch (Exception e) {
            Assert.assertEquals("Partitioned Topic not found: persistent://my-tenant/my-namespace/topic-not-found-partition-0 has zero partitions", e.getMessage());
        }
        persistentTopics.createPartitionedTopic(testTenant, testNamespace, testLocalTopicName, 3, true);
        try {
            persistentTopics.getSubscriptions(testTenant, testNamespace, testLocalTopicName + "-partition-0", true);
        } catch (Exception e) {
            Assert.assertEquals("Topic partitions were not yet created", e.getMessage());
        }
        persistentTopics.createSubscription(testTenant, testNamespace, testLocalTopicName, "test", true, (MessageIdImpl)MessageId.earliest);
        List<String> subscriptions =  persistentTopics.getSubscriptions(testTenant, testNamespace, testLocalTopicName + "-partition-0", true);
        Assert.assertTrue(subscriptions.contains("test"));
        persistentTopics.deleteSubscription(testTenant, testNamespace, testLocalTopicName, "test", true);
        subscriptions =  persistentTopics.getSubscriptions(testTenant, testNamespace, testLocalTopicName + "-partition-0", true);
        Assert.assertTrue(subscriptions.isEmpty());
        persistentTopics.deletePartitionedTopic(testTenant, testNamespace, testLocalTopicName, true, true);
    }

    @Test
    public void testNonPartitionedTopics() {
    	pulsar.getConfiguration().setAllowAutoTopicCreation(false);
    	final String nonPartitionTopic = "non-partitioned-topic";
    	persistentTopics.createSubscription(testTenant, testNamespace, nonPartitionTopic, "test", true, (MessageIdImpl) MessageId.latest);
    	try {
    		persistentTopics.getSubscriptions(testTenant, testNamespace, nonPartitionTopic + "-partition-0", true);
    	} catch (RestException exc) {
    		Assert.assertTrue(exc.getMessage().contains("zero partitions"));
    	}
    	final String nonPartitionTopic2 = "secondary-non-partitioned-topic";
    	persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, nonPartitionTopic2, true);
    	Assert.assertEquals(persistentTopics.getPartitionedMetadata(testTenant, testNamespace, nonPartitionTopic, true).partitions, 0);
    }
}
