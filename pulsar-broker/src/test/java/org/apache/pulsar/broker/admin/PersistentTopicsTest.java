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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.pulsar.broker.admin.v2.NonPersistentTopics;
import org.apache.pulsar.broker.admin.v2.PersistentTopics;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.zookeeper.KeeperException;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

@PrepareForTest(PersistentTopics.class)
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
        doReturn(mockZooKeeper).when(persistentTopics).globalZk();
        doReturn(mockZooKeeper).when(persistentTopics).localZk();
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
        doReturn(mockZooKeeper).when(nonPersistentTopic).globalZk();
        doReturn(mockZooKeeper).when(nonPersistentTopic).localZk();
        doReturn(pulsar.getConfigurationCache().propertiesCache()).when(nonPersistentTopic).tenantsCache();
        doReturn(pulsar.getConfigurationCache().policiesCache()).when(nonPersistentTopic).policiesCache();
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
        Assert.assertEquals(persistentTopics
                        .getPartitionedMetadata(testTenant, testNamespace, nonPartitionTopic, true, false).partitions,
                0);
    }

    @Test
    public void testCreateNonPartitionedTopic() {
        final String topicName = "standard-topic-partition-a";
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, topicName, true);
        PartitionedTopicMetadata pMetadata = persistentTopics.getPartitionedMetadata(
                testTenant, testNamespace, topicName, true, false);
        Assert.assertEquals(pMetadata.partitions, 0);
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
        ZooKeeperChildrenCache mockZooKeeperChildrenCache = mock(ZooKeeperChildrenCache.class);
        doReturn(mockLocalZooKeeperCacheService).when(pulsar).getLocalZkCacheService();
        doReturn(mockZooKeeperChildrenCache).when(mockLocalZooKeeperCacheService).managedLedgerListCache();
        doReturn(ImmutableSet.of(nonPartitionTopicName1, nonPartitionTopicName2)).when(mockZooKeeperChildrenCache).get(anyString());
        doReturn(CompletableFuture.completedFuture(ImmutableSet.of(nonPartitionTopicName1, nonPartitionTopicName2))).when(mockZooKeeperChildrenCache).getAsync(anyString());
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
        ZooKeeperChildrenCache mockZooKeeperChildrenCache = mock(ZooKeeperChildrenCache.class);
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
        persistentTopics.updatePartitionedTopic(testTenant, testNamespace, partitionedTopicName, true, 10);
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
        persistentTopics.deletePartitionedTopic(response, testTenant, testNamespace, partitionTopicName, true, true);
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

    @Test()
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
}
