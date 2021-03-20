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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.admin.v2.NonPersistentTopics;
import org.apache.pulsar.broker.admin.v2.PersistentTopics;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.zookeeper.ZooKeeperManagedLedgerCache;
import org.apache.zookeeper.KeeperException;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@PrepareForTest(PersistentTopics.class)
@PowerMockIgnore("com.sun.management.*")
@Slf4j
@Test(groups = "broker")
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
        doReturn(mockZooKeeper).when(persistentTopics).localZk();
        doReturn(false).when(persistentTopics).isRequestHttps();
        doReturn(null).when(persistentTopics).originalPrincipal();
        doReturn("test").when(persistentTopics).clientAppId();
        doReturn(TopicDomain.persistent.value()).when(persistentTopics).domain();
        doNothing().when(persistentTopics).validateAdminAccessForTenant(this.testTenant);
        doReturn(mock(AuthenticationDataHttps.class)).when(persistentTopics).clientAuthData();

        nonPersistentTopic = spy(new NonPersistentTopics());
        nonPersistentTopic.setServletContext(new MockServletContext());
        nonPersistentTopic.setPulsar(pulsar);
        doReturn(mockZooKeeper).when(nonPersistentTopic).localZk();
        doReturn(false).when(nonPersistentTopic).isRequestHttps();
        doReturn(null).when(nonPersistentTopic).originalPrincipal();
        doReturn("test").when(nonPersistentTopic).clientAppId();
        doReturn(TopicDomain.non_persistent.value()).when(nonPersistentTopic).domain();
        doNothing().when(nonPersistentTopic).validateAdminAccessForTenant(this.testTenant);
        doReturn(mock(AuthenticationDataHttps.class)).when(nonPersistentTopic).clientAuthData();


        admin.clusters().createCluster("use", new ClusterData("http://broker-use.com:8080"));
        admin.clusters().createCluster("test", new ClusterData("http://broker-use.com:8080"));
        admin.tenants().createTenant(this.testTenant,
                new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet(testLocalCluster, "test")));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet(testLocalCluster, "test"));
    }

    @Override
    @AfterMethod(alwaysRun = true)
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
        response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, testLocalTopicName, 3);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 4) Create a subscription
        response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, testLocalTopicName, "test", true,
                (MessageIdImpl) MessageId.earliest, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 5) Confirm that the subscription exists
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName + "-partition-0",
                true);
        verify(response, timeout(5000).times(1)).resume(Lists.newArrayList("test"));

        // 6) Delete the subscription
        response = mock(AsyncResponse.class);
        persistentTopics.deleteSubscription(response, testTenant, testNamespace, testLocalTopicName, "test", false,true);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 7) Confirm that the subscription does not exist
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName + "-partition-0",
                true);
        verify(response, timeout(5000).times(1)).resume(Lists.newArrayList());

        // 8) Delete the partitioned topic
        response = mock(AsyncResponse.class);
        persistentTopics.deletePartitionedTopic(response, testTenant, testNamespace, testLocalTopicName, true, true, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
    }



    @Test
    public void testCreateSubscriptions() throws Exception{
        final int numberOfMessages = 5;
        final String SUB_EARLIEST = "sub-earliest";
        final String SUB_LATEST = "sub-latest";
        final String SUB_NONE_MESSAGE_ID = "sub-none-message-id";

        String testLocalTopicName = "subWithPositionOrNot";
        final String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testLocalTopicName;
        admin.topics().createNonPartitionedTopic(topicName);

        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName)
                .maxPendingMessages(30000).create();

        // 1) produce numberOfMessages message to pulsar
        for (int i = 0; i < numberOfMessages; i++) {
            System.out.println(producer.send(new byte[10]));
        }

        // 2) Create a subscription from earliest position

        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, testLocalTopicName, SUB_EARLIEST, true,
                (MessageIdImpl) MessageId.earliest, false);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        TopicStats topicStats = persistentTopics.getStats(testTenant, testNamespace, testLocalTopicName, true, true, false);
        long msgBacklog = topicStats.subscriptions.get(SUB_EARLIEST).msgBacklog;
        System.out.println("Message back log for " + SUB_EARLIEST + " is :" + msgBacklog);
        Assert.assertEquals(msgBacklog, numberOfMessages);

        // 3) Create a subscription with form latest position

        response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, testLocalTopicName, SUB_LATEST, true,
                (MessageIdImpl) MessageId.latest, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        topicStats = persistentTopics.getStats(testTenant, testNamespace, testLocalTopicName, true, true, false);
        msgBacklog = topicStats.subscriptions.get(SUB_LATEST).msgBacklog;
        System.out.println("Message back log for " + SUB_LATEST + " is :" + msgBacklog);
        Assert.assertEquals(msgBacklog, 0);

        // 4) Create a subscription without position

        response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, testLocalTopicName, SUB_NONE_MESSAGE_ID, true,
                null, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        topicStats = persistentTopics.getStats(testTenant, testNamespace, testLocalTopicName, true, true, false);
        msgBacklog = topicStats.subscriptions.get(SUB_NONE_MESSAGE_ID).msgBacklog;
        System.out.println("Message back log for " + SUB_NONE_MESSAGE_ID + " is :" + msgBacklog);
        Assert.assertEquals(msgBacklog, 0);

        producer.close();
    }

    @Test
    public void testCreateSubscriptionForNonPersistentTopic() throws InterruptedException {
        doReturn(TopicDomain.non_persistent.value()).when(persistentTopics).domain();
        AsyncResponse response  = mock(AsyncResponse.class);
        ArgumentCaptor<WebApplicationException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace,
                "testCreateSubscriptionForNonPersistentTopic", "sub",
                true, (MessageIdImpl) MessageId.earliest, false);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testTerminatePartitionedTopic() {
        String testLocalTopicName = "topic-not-found";

        // 3) Create the partitioned topic
        AsyncResponse response  = mock(AsyncResponse.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, testLocalTopicName, 1);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 5) Create a subscription
        response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, testLocalTopicName, "test", true,
                (MessageIdImpl) MessageId.earliest, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 9) terminate partitioned topic
        response = mock(AsyncResponse.class);
        persistentTopics.terminatePartitionedTopic(response, testTenant, testNamespace, testLocalTopicName, true);
        verify(response, timeout(5000).times(1)).resume(Arrays.asList(new MessageIdImpl(3, -1, -1)));
    }

    @Test
    public void testNonPartitionedTopics() {
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
        Assert.assertEquals(persistentTopics
                        .getPartitionedMetadata(testTenant, testNamespace, nonPartitionTopic, true, false).partitions,
                0);

        Assert.assertEquals(persistentTopics
                        .getPartitionedMetadata(testTenant, testNamespace, nonPartitionTopic, true, true).partitions,
                0);
    }

    @Test
    public void testCreateNonPartitionedTopic() {
        final String topicName = "standard-topic-partition-a";
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);
        PartitionedTopicMetadata pMetadata = persistentTopics.getPartitionedMetadata(
                testTenant, testNamespace, topicName, true, false);
        Assert.assertEquals(pMetadata.partitions, 0);

        PartitionedTopicMetadata metadata = persistentTopics.getPartitionedMetadata(
                testTenant, testNamespace, topicName, true, true);
        Assert.assertEquals(metadata.partitions, 0);
    }

    @Test(expectedExceptions = RestException.class)
    public void testCreateNonPartitionedTopicWithInvalidName() {
        final String topicName = "standard-topic-partition-10";
        doAnswer(invocation -> {
            TopicName partitionedTopicname = invocation.getArgument(0, TopicName.class);
            assert(partitionedTopicname.getLocalName().equals("standard-topic"));
            return new PartitionedTopicMetadata(10);
        }).when(persistentTopics).getPartitionedTopicMetadata(any(), anyBoolean(), anyBoolean());
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);
    }

    @Test
    public void testCreatePartitionedTopicHavingNonPartitionTopicWithPartitionSuffix() throws KeeperException, InterruptedException {
        // Test the case in which user already has topic like topic-name-partition-123 created before we enforce the validation.
        final String nonPartitionTopicName1 = "standard-topic";
        final String nonPartitionTopicName2 = "special-topic-partition-123";
        final String partitionedTopicName = "special-topic";
        LocalZooKeeperCacheService mockLocalZooKeeperCacheService = mock(LocalZooKeeperCacheService.class);
        ZooKeeperManagedLedgerCache mockZooKeeperChildrenCache = mock(ZooKeeperManagedLedgerCache.class);
        doReturn(mockLocalZooKeeperCacheService).when(pulsar).getLocalZkCacheService();
        doReturn(mockZooKeeperChildrenCache).when(mockLocalZooKeeperCacheService).managedLedgerListCache();
        doReturn(ImmutableSet.of(nonPartitionTopicName1, nonPartitionTopicName2)).when(mockZooKeeperChildrenCache).get(anyString());
        doReturn(CompletableFuture.completedFuture(ImmutableSet.of(nonPartitionTopicName1, nonPartitionTopicName2))).when(mockZooKeeperChildrenCache).getAsync(anyString());
        doReturn(new Policies()).when(persistentTopics).getNamespacePolicies(any());
        AsyncResponse response = mock(AsyncResponse.class);
        ArgumentCaptor<RestException> errCaptor = ArgumentCaptor.forClass(RestException.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionedTopicName, 5);
        verify(response, timeout(5000).times(1)).resume(errCaptor.capture());
        Assert.assertEquals(errCaptor.getValue().getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
    }

    @Test(expectedExceptions = RestException.class)
    public void testUpdatePartitionedTopicHavingNonPartitionTopicWithPartitionSuffix() throws Exception {
        // Already have non partition topic special-topic-partition-10, shouldn't able to update number of partitioned topic to more than 10.
        final String nonPartitionTopicName2 = "special-topic-partition-10";
        final String partitionedTopicName = "special-topic";
        LocalZooKeeperCacheService mockLocalZooKeeperCacheService = mock(LocalZooKeeperCacheService.class);
        ZooKeeperManagedLedgerCache mockZooKeeperChildrenCache = mock(ZooKeeperManagedLedgerCache.class);
        doReturn(mockLocalZooKeeperCacheService).when(pulsar).getLocalZkCacheService();
        doReturn(mockZooKeeperChildrenCache).when(mockLocalZooKeeperCacheService).managedLedgerListCache();
        doReturn(ImmutableSet.of(nonPartitionTopicName2)).when(mockZooKeeperChildrenCache).get(anyString());
        doReturn(CompletableFuture.completedFuture(ImmutableSet.of(nonPartitionTopicName2))).when(mockZooKeeperChildrenCache).getAsync(anyString());
        doAnswer(invocation -> {
            persistentTopics.namespaceName = NamespaceName.get("tenant", "namespace");
            persistentTopics.topicName = TopicName.get("persistent", "tenant", "cluster", "namespace", "topicname");
            return null;
        }).when(persistentTopics).validatePartitionedTopicName(any(), any(), any());
        doNothing().when(persistentTopics).validateAdminAccessForTenant(anyString());
        AsyncResponse response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionedTopicName, 5);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        persistentTopics.updatePartitionedTopic(testTenant, testNamespace, partitionedTopicName, true, false, 10);
    }

    @Test
    public void testUnloadTopic() {
        final String topicName = "standard-topic-to-be-unload";
        final String partitionTopicName = "partition-topic-to-be-unload";

        // 1) not exist topic
        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.unloadTopic(response, testTenant, testNamespace, "topic-not-exist", true);
        ArgumentCaptor<RestException> errCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(errCaptor.capture());
        Assert.assertEquals(errCaptor.getValue().getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());

        // 2) create non partitioned topic and unload
        response = mock(AsyncResponse.class);
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);
        persistentTopics.unloadTopic(response, testTenant, testNamespace, topicName, true);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 3) create partitioned topic and unload
        response = mock(AsyncResponse.class);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionTopicName, 6);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        response = mock(AsyncResponse.class);
        persistentTopics.unloadTopic(response, testTenant, testNamespace, partitionTopicName, true);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 4) delete partitioned topic
        response = mock(AsyncResponse.class);
        persistentTopics.deletePartitionedTopic(response, testTenant, testNamespace, partitionTopicName, true, true, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
    }

    @Test
    public void testUnloadTopicShallThrowNotFoundWhenTopicNotExist() {
        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.unloadTopic(response, testTenant, testNamespace,"non-existent-topic", true);
        ArgumentCaptor<RestException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void testGetPartitionedTopicsList() throws KeeperException, InterruptedException, PulsarAdminException {
        AsyncResponse response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, "test-topic1", 3);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        response = mock(AsyncResponse.class);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        nonPersistentTopic.createPartitionedTopic(response, testTenant, testNamespace, "test-topic2", 3);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        List<String> persistentPartitionedTopics = persistentTopics.getPartitionedTopicList(testTenant, testNamespace);

        Assert.assertEquals(persistentPartitionedTopics.size(), 1);
        Assert.assertEquals(TopicName.get(persistentPartitionedTopics.get(0)).getDomain().value(), TopicDomain.persistent.value());

        List<String> nonPersistentPartitionedTopics = nonPersistentTopic.getPartitionedTopicList(testTenant, testNamespace);
        Assert.assertEquals(nonPersistentPartitionedTopics.size(), 1);
        Assert.assertEquals(TopicName.get(nonPersistentPartitionedTopics.get(0)).getDomain().value(), TopicDomain.non_persistent.value());
    }

    @Test
    public void testGrantNonPartitionedTopic() {
        final String topicName = "non-partitioned-topic";
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);
        String role = "role";
        Set<AuthAction> expectActions = new HashSet<>();
        expectActions.add(AuthAction.produce);
        persistentTopics.grantPermissionsOnTopic(testTenant, testNamespace, topicName, role, expectActions);
        Map<String, Set<AuthAction>> permissions = persistentTopics.getPermissionsOnTopic(testTenant, testNamespace, topicName);
        Assert.assertEquals(permissions.get(role), expectActions);
    }

    @Test
    public void testCreateExistedPartition() {
        final AsyncResponse response = mock(AsyncResponse.class);
        final String topicName = "test-create-existed-partition";
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, topicName, 3);

        final String partitionName = TopicName.get(topicName).getPartition(0).getLocalName();
        try {
            persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, partitionName, false);
            Assert.fail();
        } catch (RestException e) {
            log.error("Failed to create {}: {}", partitionName, e.getMessage());
            Assert.assertEquals(e.getResponse().getStatus(), 409);
            Assert.assertEquals(e.getMessage(), "This topic already exists");
        }
    }

    @Test
    public void testGrantPartitionedTopic() {
        final String partitionedTopicName = "partitioned-topic";
        final int numPartitions = 5;
        AsyncResponse response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionedTopicName, numPartitions);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        String role = "role";
        Set<AuthAction> expectActions = new HashSet<>();
        expectActions.add(AuthAction.produce);
        persistentTopics.grantPermissionsOnTopic(testTenant, testNamespace, partitionedTopicName, role, expectActions);
        Map<String, Set<AuthAction>> permissions = persistentTopics.getPermissionsOnTopic(testTenant, testNamespace,
                partitionedTopicName);
        Assert.assertEquals(permissions.get(role), expectActions);
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(), testTenant, testNamespace,
                partitionedTopicName);
        for (int i = 0; i < numPartitions; i++) {
            TopicName partition = topicName.getPartition(i);
            Map<String, Set<AuthAction>> partitionPermissions = persistentTopics.getPermissionsOnTopic(testTenant,
                    testNamespace, partition.getEncodedLocalName());
            Assert.assertEquals(partitionPermissions.get(role), expectActions);
        }
    }

    @Test
    public void testRevokeNonPartitionedTopic() {
        final String topicName = "non-partitioned-topic";
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);
        String role = "role";
        Set<AuthAction> expectActions = new HashSet<>();
        expectActions.add(AuthAction.produce);
        persistentTopics.grantPermissionsOnTopic(testTenant, testNamespace, topicName, role, expectActions);
        persistentTopics.revokePermissionsOnTopic(testTenant, testNamespace, topicName, role);
        Map<String, Set<AuthAction>> permissions = persistentTopics.getPermissionsOnTopic(testTenant, testNamespace, topicName);
        Assert.assertEquals(permissions.get(role), null);
    }

    @Test
    public void testRevokePartitionedTopic() {
        final String partitionedTopicName = "partitioned-topic";
        final int numPartitions = 5;
        AsyncResponse response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionedTopicName, numPartitions);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        String role = "role";
        Set<AuthAction> expectActions = new HashSet<>();
        expectActions.add(AuthAction.produce);
        persistentTopics.grantPermissionsOnTopic(testTenant, testNamespace, partitionedTopicName, role, expectActions);
        persistentTopics.revokePermissionsOnTopic(testTenant, testNamespace, partitionedTopicName, role);
        Map<String, Set<AuthAction>> permissions = persistentTopics.getPermissionsOnTopic(testTenant, testNamespace,
                partitionedTopicName);
        Assert.assertEquals(permissions.get(role), null);
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(), testTenant, testNamespace,
                partitionedTopicName);
        for (int i = 0; i < numPartitions; i++) {
            TopicName partition = topicName.getPartition(i);
            Map<String, Set<AuthAction>> partitionPermissions = persistentTopics.getPermissionsOnTopic(testTenant,
                    testNamespace, partition.getEncodedLocalName());
            Assert.assertEquals(partitionPermissions.get(role), null);
        }
    }

    @Test
    public void testTriggerCompactionTopic() {
        final String partitionTopicName = "test-part";
        final String nonPartitionTopicName = "test-non-part";

        // trigger compaction on non-existing topic
        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.compact(response, testTenant, testNamespace, "non-existing-topic", true);
        ArgumentCaptor<RestException> errCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(5000).times(1)).resume(errCaptor.capture());
        Assert.assertEquals(errCaptor.getValue().getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());

        // create non partitioned topic and compaction on it
        response = mock(AsyncResponse.class);
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, nonPartitionTopicName, true);
        persistentTopics.compact(response, testTenant, testNamespace, nonPartitionTopicName, true);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // create partitioned topic and compaction on it
        response = mock(AsyncResponse.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionTopicName, 2);
        persistentTopics.compact(response, testTenant, testNamespace, partitionTopicName, true);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
    }

    @Test
    public void testPeekWithSubscriptionNameNotExist() throws Exception {
        final String topicName = "testTopic";
        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                testTenant,
                testNamespace,
                topicName).toString();
        final String subscriptionName = "sub";

        admin.topics().createPartitionedTopic(topic, 3);

        final String partitionedTopic = topic + "-partition-0";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        for (int i = 0; i < 100; ++i) {
            producer.send("test" + i);
        }

        List<Message<byte[]>> messages = admin.topics()
                .peekMessages(partitionedTopic, subscriptionName, 5);

        Assert.assertEquals(messages.size(), 5);

        producer.close();
    }

    @Test
    public void testGetLastMessageId() throws Exception {
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
        final String topicName = "persistent://prop-xyz/ns1/testGetLastMessageId";

        admin.topics().createNonPartitionedTopic(topicName);
        Producer<byte[]> batchProducer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(true)
                .batchingMaxMessages(100)
                .batchingMaxPublishDelay(2, TimeUnit.SECONDS)
                .create();
        admin.topics().createSubscription(topicName, "test", MessageId.earliest);
        CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
        for (int i = 0; i < 10; i++) {
            completableFuture = batchProducer.sendAsync("test".getBytes());
        }
        completableFuture.get();
        Assert.assertEquals(((BatchMessageIdImpl) admin.topics().getLastMessageId(topicName)).getBatchIndex(), 9);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .create();
        producer.send("test".getBytes());

        Assert.assertTrue(admin.topics().getLastMessageId(topicName) instanceof MessageIdImpl);

    }

    @Test
    public void testExamineMessage() throws Exception {
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("tenant-xyz", tenantInfo);
        admin.namespaces().createNamespace("tenant-xyz/ns-abc", Sets.newHashSet("test"));
        final String topicName = "persistent://tenant-xyz/ns-abc/topic-123";

        admin.topics().createPartitionedTopic(topicName, 2);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName + "-partition-0").create();

        // Check examine message not allowed on partitioned topic.
        try {
            admin.topics().examineMessage(topicName, "earliest", 1);
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getMessage(), "Examine messages on a partitioned topic is not allowed, please try examine message on specific topic partition");
        }

        producer.send("message1");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "earliest", 1).getData()), "message1");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "latest", 1).getData()), "message1");

        producer.send("message2");
        producer.send("message3");
        producer.send("message4");
        producer.send("message5");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "earliest", 1).getData()), "message1");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "earliest", 2).getData()), "message2");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "earliest", 3).getData()), "message3");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "earliest", 4).getData()), "message4");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "earliest", 5).getData()), "message5");

        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "latest", 1).getData()), "message5");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "latest", 2).getData()), "message4");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "latest", 3).getData()), "message3");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "latest", 4).getData()), "message2");
        Assert.assertEquals(new String(admin.topics().examineMessage(topicName + "-partition-0", "latest", 5).getData()), "message1");
    }

    @Test
    public void testOffloadWithNullMessageId() {
        final String topicName = "topic-123";
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);

        try {
            persistentTopics.triggerOffload(testTenant, testNamespace, topicName, true, null);
            Assert.fail("should have failed");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        }
    }
}
