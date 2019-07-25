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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.pulsar.broker.admin.v2.NonPersistentTopics;
import org.apache.pulsar.broker.admin.v2.PersistentTopics;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.zookeeper.KeeperException;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.lang.reflect.Field;
import java.util.List;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PersistentTopicsTest extends MockedPulsarServiceBaseTest {

    private PersistentTopics persistentTopics;
    private final String testTenant = "my-tenant";
    private final String testLocalCluster = "use";
    private final String testNamespace = "my-namespace";
    protected Field uriField;
    protected UriInfo uriInfo;
    private NonPersistentTopics nonPersistentTopic;

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
        doReturn(TopicDomain.persistent.value()).when(persistentTopics).domain();
        doNothing().when(persistentTopics).validateAdminAccessForTenant(this.testTenant);
        doReturn(mock(AuthenticationDataHttps.class)).when(persistentTopics).clientAuthData();

        nonPersistentTopic = spy(new NonPersistentTopics());
        nonPersistentTopic.setServletContext(new MockServletContext());
        nonPersistentTopic.setPulsar(pulsar);
        doReturn(mockZookKeeper).when(nonPersistentTopic).globalZk();
        doReturn(mockZookKeeper).when(nonPersistentTopic).localZk();
        doReturn(pulsar.getConfigurationCache().propertiesCache()).when(nonPersistentTopic).tenantsCache();
        doReturn(pulsar.getConfigurationCache().policiesCache()).when(nonPersistentTopic).policiesCache();
        doReturn(false).when(nonPersistentTopic).isRequestHttps();
        doReturn(null).when(nonPersistentTopic).originalPrincipal();
        doReturn("test").when(nonPersistentTopic).clientAppId();
        doReturn(TopicDomain.non_persistent.value()).when(nonPersistentTopic).domain();
        doNothing().when(nonPersistentTopic).validateAdminAccessForTenant(this.testTenant);
        doReturn(mock(AuthenticationDataHttps.class)).when(nonPersistentTopic).clientAuthData();


        admin.clusters().createCluster("use", new ClusterData("http://broker-use.com:" + BROKER_WEBSERVICE_PORT));
        admin.clusters().createCluster("test", new ClusterData("http://broker-use.com:" + BROKER_WEBSERVICE_PORT));
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

        // 1) Confirm that the topic does not exist
        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName, true);
        ArgumentCaptor<RestException> errorCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(errorCaptor.capture());
        Assert.assertEquals(errorCaptor.getValue().getResponse().getStatus(),
                Response.Status.NOT_FOUND.getStatusCode());
        Assert.assertEquals(errorCaptor.getValue().getMessage(), "Topic not found");

        // 2) Confirm that the partitioned topic does not exist
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName + "-partition-0",
                true);
        errorCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(errorCaptor.capture());
        Assert.assertEquals(errorCaptor.getValue().getResponse().getStatus(),
                Response.Status.NOT_FOUND.getStatusCode());
        Assert.assertEquals(errorCaptor.getValue().getMessage(),
                "Partitioned Topic not found: persistent://my-tenant/my-namespace/topic-not-found-partition-0 has zero partitions");

        // 3) Create the partitioned topic
        persistentTopics.createPartitionedTopic(testTenant, testNamespace, testLocalTopicName, 3);

        // 4) Confirm that the topic partitions has not been created yet
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName + "-partition-0",
                true);
        errorCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(errorCaptor.capture());
        Assert.assertEquals(errorCaptor.getValue().getResponse().getStatus(),
                Response.Status.NOT_FOUND.getStatusCode());
        Assert.assertEquals(errorCaptor.getValue().getMessage(), "Topic partitions were not yet created");

        // 5) Create a subscription
        response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, testLocalTopicName, "test", true,
                (MessageIdImpl) MessageId.earliest, false);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 6) Confirm that the subscription exists
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName + "-partition-0",
                true);
        verify(response, timeout(5000).times(1)).resume(Lists.newArrayList("test"));

        // 7) Delete the subscription
        response = mock(AsyncResponse.class);
        persistentTopics.deleteSubscription(response, testTenant, testNamespace, testLocalTopicName, "test", true);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 8) Confirm that the subscription does not exist
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName + "-partition-0",
                true);
        verify(response, timeout(5000).times(1)).resume(Lists.newArrayList());

        // 9) Delete the partitioned topic
        response = mock(AsyncResponse.class);
        persistentTopics.deletePartitionedTopic(response, testTenant, testNamespace, testLocalTopicName, true, true);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
    }

    @Test
    public void testNonPartitionedTopics() {
        pulsar.getConfiguration().setAllowAutoTopicCreation(false);
        final String nonPartitionTopic = "non-partitioned-topic";
        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, nonPartitionTopic, "test", true,
                (MessageIdImpl) MessageId.latest, false);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, nonPartitionTopic + "-partition-0",
                true);
        ArgumentCaptor<RestException> errorCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(errorCaptor.capture());
        Assert.assertTrue(errorCaptor.getValue().getMessage().contains("zero partitions"));

        final String nonPartitionTopic2 = "secondary-non-partitioned-topic";
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, nonPartitionTopic2, true);
        Assert.assertEquals(
                persistentTopics.getPartitionedMetadata(testTenant, testNamespace, nonPartitionTopic, true) .partitions,
                0);
    }

    @Test
    public void testCreateNonPartitionedTopic() {
        final String topicName = "standard-topic";
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);
        PartitionedTopicMetadata pMetadata = persistentTopics.getPartitionedMetadata(
                testTenant, testNamespace, topicName, true);
        Assert.assertEquals(pMetadata.partitions, 0);
    }

    @Test
    public void testUnloadTopic() {
        final String topicName = "standard-topic-to-be-unload";
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);
        persistentTopics.unloadTopic(testTenant, testNamespace, topicName, true);
    }

    @Test(expectedExceptions = RestException.class)
    public void testUnloadTopicShallThrowNotFoundWhenTopicNotExist() {
        try {
            persistentTopics.unloadTopic(testTenant, testNamespace,"non-existent-topic", true);
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
            throw e;
        }
    }

    @Test
    public void testGetPartitionedTopicsList() throws KeeperException, InterruptedException, PulsarAdminException {

        persistentTopics.createPartitionedTopic(testTenant, testNamespace, "test-topic1", 3);

        nonPersistentTopic.createPartitionedTopic(testTenant, testNamespace, "test-topic2", 3);

        List<String> persistentPartitionedTopics = persistentTopics.getPartitionedTopicList(testTenant, testNamespace);

        Assert.assertEquals(persistentPartitionedTopics.size(), 1);
        Assert.assertEquals(TopicName.get(persistentPartitionedTopics.get(0)).getDomain().value(), TopicDomain.persistent.value());

        List<String> nonPersistentPartitionedTopics = nonPersistentTopic.getPartitionedTopicList(testTenant, testNamespace);
        Assert.assertEquals(nonPersistentPartitionedTopics.size(), 1);
        Assert.assertEquals(TopicName.get(nonPersistentPartitionedTopics.get(0)).getDomain().value(), TopicDomain.non_persistent.value());
    }
}
