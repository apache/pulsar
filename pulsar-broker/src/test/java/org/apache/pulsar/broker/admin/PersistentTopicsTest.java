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
import static org.mockito.Mockito.when;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.admin.v2.NonPersistentTopics;
import org.apache.pulsar.broker.admin.v2.PersistentTopics;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.zookeeper.KeeperException;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;
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
        doReturn(false).when(persistentTopics).isRequestHttps();
        doReturn(null).when(persistentTopics).originalPrincipal();
        doReturn("test").when(persistentTopics).clientAppId();
        doReturn(TopicDomain.persistent.value()).when(persistentTopics).domain();
        doNothing().when(persistentTopics).validateAdminAccessForTenant(this.testTenant);
        doReturn(mock(AuthenticationDataHttps.class)).when(persistentTopics).clientAuthData();

        nonPersistentTopic = spy(new NonPersistentTopics());
        nonPersistentTopic.setServletContext(new MockServletContext());
        nonPersistentTopic.setPulsar(pulsar);
        doReturn(false).when(nonPersistentTopic).isRequestHttps();
        doReturn(null).when(nonPersistentTopic).originalPrincipal();
        doReturn("test").when(nonPersistentTopic).clientAppId();
        doReturn(TopicDomain.non_persistent.value()).when(nonPersistentTopic).domain();
        doNothing().when(nonPersistentTopic).validateAdminAccessForTenant(this.testTenant);
        doReturn(mock(AuthenticationDataHttps.class)).when(nonPersistentTopic).clientAuthData();

        PulsarResources resources =
                spy(new PulsarResources(pulsar.getLocalMetadataStore(), pulsar.getConfigurationMetadataStore()));
        doReturn(spy(new TopicResources(pulsar.getLocalMetadataStore()))).when(resources).getTopicResources();
        Whitebox.setInternalState(pulsar, "pulsarResources", resources);

        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl("http://broker-use.com:8080").build());
        admin.clusters().createCluster("test",ClusterData.builder().serviceUrl("http://broker-use.com:8080").build());
        admin.tenants().createTenant(this.testTenant,
                new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet(testLocalCluster, "test")));
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
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, testLocalTopicName, 3, true);
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

        // 8) Create a sub of partitioned-topic
        response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, testLocalTopicName + "-partition-1", "test", true,
                (MessageIdImpl) MessageId.earliest, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        //
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName + "-partition-1", true);
        verify(response, timeout(5000).times(1)).resume(Lists.newArrayList("test"));
        //
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName + "-partition-0", true);
        verify(response, timeout(5000).times(1)).resume(Lists.newArrayList());
        //
        response = mock(AsyncResponse.class);
        persistentTopics.getSubscriptions(response, testTenant, testNamespace, testLocalTopicName, true);
        verify(response, timeout(5000).times(1)).resume(Lists.newArrayList("test"));

        // 9) Delete the partitioned topic
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
        long msgBacklog = topicStats.getSubscriptions().get(SUB_EARLIEST).getMsgBacklog();
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
        msgBacklog = topicStats.getSubscriptions().get(SUB_LATEST).getMsgBacklog();
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
        msgBacklog = topicStats.getSubscriptions().get(SUB_NONE_MESSAGE_ID).getMsgBacklog();
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
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, testLocalTopicName, 1, true);
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
    public void testTerminate() {
        String testLocalTopicName = "topic-not-found";

        // 1) Create the nonPartitionTopic topic
        persistentTopics.createNonPartitionedTopic(testTenant, testNamespace, testLocalTopicName, true);

        // 2) Create a subscription
        AsyncResponse response  = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, testLocalTopicName, "test", true,
                (MessageIdImpl) MessageId.earliest, false);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 3) Assert terminate persistent topic
        MessageId messageId = persistentTopics.terminate(testTenant, testNamespace, testLocalTopicName, true);
        Assert.assertEquals(messageId, new MessageIdImpl(3, -1, -1));

        // 4) Assert terminate non-persistent topic
        String nonPersistentTopicName = "non-persistent-topic";
        try {
            nonPersistentTopic.terminate(testTenant, testNamespace, nonPersistentTopicName, true);
            Assert.fail("Should fail validation on non-persistent topic");
        } catch (RestException e) {
            Assert.assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), e.getResponse().getStatus());
        }
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

        when(pulsar.getPulsarResources().getTopicResources()
                .listPersistentTopicsAsync(NamespaceName.get("my-tenant/my-namespace")))
                .thenReturn(CompletableFuture.completedFuture(Lists.newArrayList(
                        "persistent://my-tenant/my-namespace/" + nonPartitionTopicName1,
                        "persistent://my-tenant/my-namespace/" + nonPartitionTopicName2
                )));
//        doReturn(ImmutableSet.of(nonPartitionTopicName1, nonPartitionTopicName2)).when(mockZooKeeperChildrenCache).get(anyString());
//        doReturn(CompletableFuture.completedFuture(ImmutableSet.of(nonPartitionTopicName1, nonPartitionTopicName2))).when(mockZooKeeperChildrenCache).getAsync(anyString());
        doReturn(new Policies()).when(persistentTopics).getNamespacePolicies(any());
        AsyncResponse response = mock(AsyncResponse.class);
        ArgumentCaptor<RestException> errCaptor = ArgumentCaptor.forClass(RestException.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionedTopicName, 5, true);
        verify(response, timeout(5000).times(1)).resume(errCaptor.capture());
        Assert.assertEquals(errCaptor.getValue().getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
    }

    @Test(expectedExceptions = RestException.class)
    public void testUpdatePartitionedTopicHavingNonPartitionTopicWithPartitionSuffix() throws Exception {
        // Already have non partition topic special-topic-partition-10, shouldn't able to update number of partitioned topic to more than 10.
        final String nonPartitionTopicName2 = "special-topic-partition-10";
        final String partitionedTopicName = "special-topic";
        pulsar.getBrokerService().getManagedLedgerFactory().open(TopicName.get(nonPartitionTopicName2).getPersistenceNamingEncoding());
        doAnswer(invocation -> {
            persistentTopics.namespaceName = NamespaceName.get("tenant", "namespace");
            persistentTopics.topicName = TopicName.get("persistent", "tenant", "cluster", "namespace", "topicname");
            return null;
        }).when(persistentTopics).validatePartitionedTopicName(any(), any(), any());

        doNothing().when(persistentTopics).validateAdminAccessForTenant(anyString());
        AsyncResponse response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionedTopicName, 5, true);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        persistentTopics.updatePartitionedTopic(testTenant, testNamespace, partitionedTopicName, false, false, false,
                10);
    }

    @Test(timeOut = 10_000)
    public void testUnloadTopic() {
        final String topicName = "standard-topic-to-be-unload";
        final String partitionTopicName = "partition-topic-to-be-unload";

        // 1) not exist topic
        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.unloadTopic(response, testTenant, testNamespace, "topic-not-exist", true);
        ArgumentCaptor<RestException> errCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(45_000).times(1)).resume(errCaptor.capture());
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
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionTopicName, 6, true);
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

    @Test(timeOut = 10_000)
    public void testUnloadTopicShallThrowNotFoundWhenTopicNotExist() {
        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.unloadTopic(response, testTenant, testNamespace,"non-existent-topic", true);
        ArgumentCaptor<RestException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(45_000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void testGetPartitionedTopicsList() throws KeeperException, InterruptedException, PulsarAdminException {
        AsyncResponse response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, "test-topic1", 3, true);
        verify(response, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        response = mock(AsyncResponse.class);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        nonPersistentTopic.createPartitionedTopic(response, testTenant, testNamespace, "test-topic2", 3, true);
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
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, topicName, 3, true);

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
        persistentTopics.createPartitionedTopic(
                response, testTenant, testNamespace, partitionedTopicName, numPartitions, true);
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
        persistentTopics.createPartitionedTopic(
                response, testTenant, testNamespace, partitionedTopicName, numPartitions, true);
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
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, partitionTopicName, 2, true);
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

        ((TopicsImpl) admin.topics()).createPartitionedTopicAsync(topic, 3, true).get();

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
    public void testGetBacklogSizeByMessageId() throws Exception{
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
        final String topicName = "persistent://prop-xyz/ns1/testGetBacklogSize";

        admin.topics().createPartitionedTopic(topicName, 1);
        Producer<byte[]> batchProducer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .create();
        CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
        for (int i = 0; i < 10; i++) {
            completableFuture = batchProducer.sendAsync("a".getBytes());
        }
        completableFuture.get();
        Assert.assertEquals(Optional.ofNullable(admin.topics().getBacklogSizeByMessageId(topicName + "-partition-0", MessageId.earliest)), Optional.of(350L));
    }

    @Test
    public void testGetLastMessageId() throws Exception {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
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
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
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
    public void testExamineMessageMetadata() throws Exception {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("tenant-xyz", tenantInfo);
        admin.namespaces().createNamespace("tenant-xyz/ns-abc", Sets.newHashSet("test"));
        final String topicName = "persistent://tenant-xyz/ns-abc/topic-testExamineMessageMetadata";

        admin.topics().createPartitionedTopic(topicName, 2);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .producerName("testExamineMessageMetadataProducer")
                .compressionType(CompressionType.LZ4)
                .topic(topicName + "-partition-0")
                .create();

        producer.newMessage()
                .keyBytes("partition123".getBytes())
                .orderingKey(new byte[]{0})
                .replicationClusters(Lists.newArrayList("a", "b"))
                .sequenceId(112233)
                .value("data")
                .send();

        MessageImpl<byte[]> message = (MessageImpl<byte[]>) admin.topics().examineMessage(
                topicName + "-partition-0", "earliest", 1);

        //test long
        Assert.assertEquals(112233, message.getSequenceId());
        //test byte[]
        Assert.assertEquals(new byte[]{0}, message.getOrderingKey());
        //test bool and byte[]
        Assert.assertEquals("partition123".getBytes(), message.getKeyBytes());
        Assert.assertTrue(message.hasBase64EncodedKey());
        //test arrays
        Assert.assertEquals(Lists.newArrayList("a", "b"), message.getReplicateTo());
        //test string
        Assert.assertEquals(producer.getProducerName(), message.getProducerName());
        //test enum
        Assert.assertEquals(CompressionType.LZ4.ordinal(), message.getMessageBuilder().getCompression().ordinal());

        Assert.assertEquals("data", new String(message.getData()));
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

    @Test
    public void testSetReplicatedSubscriptionStatus() {
        final String topicName = "topic-with-repl-sub";
        final String partitionName = topicName + "-partition-0";
        final String subName = "sub";

        // 1) Return 404 if that the topic does not exist
        AsyncResponse response = mock(AsyncResponse.class);
        persistentTopics.setReplicatedSubscriptionStatus(response, testTenant, testNamespace, topicName, subName, true,
                true);
        ArgumentCaptor<RestException> errorCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(10000).times(1)).resume(errorCaptor.capture());
        Assert.assertEquals(errorCaptor.getValue().getResponse().getStatus(),
                Response.Status.NOT_FOUND.getStatusCode());

        // 2) Return 404 if that the partitioned topic does not exist
        response = mock(AsyncResponse.class);
        persistentTopics.setReplicatedSubscriptionStatus(response, testTenant, testNamespace, partitionName, subName,
                true, true);
        errorCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(response, timeout(10000).times(1)).resume(errorCaptor.capture());
        Assert.assertEquals(errorCaptor.getValue().getResponse().getStatus(),
                Response.Status.NOT_FOUND.getStatusCode());

        // 3) Create the partitioned topic
        response = mock(AsyncResponse.class);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        persistentTopics.createPartitionedTopic(response, testTenant, testNamespace, topicName, 2,true);
        verify(response, timeout(10000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 4) Create a subscription
        response = mock(AsyncResponse.class);
        persistentTopics.createSubscription(response, testTenant, testNamespace, topicName, subName, true,
                (MessageIdImpl) MessageId.latest, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(10000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 5) Enable replicated subscription on the partitioned topic
        response = mock(AsyncResponse.class);
        persistentTopics.setReplicatedSubscriptionStatus(response, testTenant, testNamespace, topicName, subName, true,
                true);
        verify(response, timeout(10000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 6) Disable replicated subscription on the partitioned topic
        response = mock(AsyncResponse.class);
        persistentTopics.setReplicatedSubscriptionStatus(response, testTenant, testNamespace, topicName, subName, true,
                false);
        verify(response, timeout(10000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 7) Enable replicated subscription on the partition
        response = mock(AsyncResponse.class);
        persistentTopics.setReplicatedSubscriptionStatus(response, testTenant, testNamespace, partitionName, subName,
                true, true);
        verify(response, timeout(10000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 8) Disable replicated subscription on the partition
        response = mock(AsyncResponse.class);
        persistentTopics.setReplicatedSubscriptionStatus(response, testTenant, testNamespace, partitionName, subName,
                true, false);
        verify(response, timeout(10000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 9) Delete the subscription
        response = mock(AsyncResponse.class);
        persistentTopics.deleteSubscription(response, testTenant, testNamespace, topicName, subName, false, true);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(10000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        // 10) Delete the partitioned topic
        response = mock(AsyncResponse.class);
        persistentTopics.deletePartitionedTopic(response, testTenant, testNamespace, topicName, true, true, false);
        responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(response, timeout(10000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.NO_CONTENT.getStatusCode());
    }

    public void testGetMessageById() throws Exception {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("tenant-xyz", tenantInfo);
        admin.namespaces().createNamespace("tenant-xyz/ns-abc", Sets.newHashSet("test"));
        final String topicName1 = "persistent://tenant-xyz/ns-abc/testGetMessageById1";
        final String topicName2 = "persistent://tenant-xyz/ns-abc/testGetMessageById2";
        admin.topics().createNonPartitionedTopic(topicName1);
        admin.topics().createNonPartitionedTopic(topicName2);
        ProducerBase<byte[]> producer1 = (ProducerBase<byte[]>) pulsarClient.newProducer().topic(topicName1)
                .enableBatching(false).create();
        String data1 = "test1";
        MessageIdImpl id1 = (MessageIdImpl) producer1.send(data1.getBytes());

        ProducerBase<byte[]> producer2 = (ProducerBase<byte[]>) pulsarClient.newProducer().topic(topicName2)
                .enableBatching(false).create();
        String data2 = "test2";
        MessageIdImpl id2 = (MessageIdImpl) producer2.send(data2.getBytes());

        Message<byte[]> message1 = admin.topics().getMessageById(topicName1, id1.getLedgerId(), id1.getEntryId());
        Assert.assertEquals(message1.getData(), data1.getBytes());

        Message<byte[]> message2 = admin.topics().getMessageById(topicName2, id2.getLedgerId(), id2.getEntryId());
        Assert.assertEquals(message2.getData(), data2.getBytes());

        Message<byte[]> message3 = null;
        try {
            message3 = admin.topics().getMessageById(topicName2, id1.getLedgerId(), id1.getEntryId());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertNull(message3);
        }

        Message<byte[]> message4 = null;
        try {
            message4 = admin.topics().getMessageById(topicName1, id2.getLedgerId(), id2.getEntryId());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertNull(message4);
        }
    }

    @Test
    public void testGetMessageIdByTimestamp() throws Exception {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("tenant-xyz", tenantInfo);
        admin.namespaces().createNamespace("tenant-xyz/ns-abc", Sets.newHashSet("test"));
        final String topicName = "persistent://tenant-xyz/ns-abc/testGetMessageIdByTimestamp";
        admin.topics().createNonPartitionedTopic(topicName);

        AtomicLong publishTime = new AtomicLong(0);
        ProducerBase<byte[]> producer = (ProducerBase<byte[]>) pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .intercept(new ProducerInterceptor() {
                    @Override
                    public void close() {

                    }

                    @Override
                    public boolean eligible(Message message) {
                        return true;
                    }

                    @Override
                    public Message beforeSend(Producer producer, Message message) {
                        return message;
                    }

                    @Override
                    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId,
                                                      Throwable exception) {
                        publishTime.set(message.getPublishTime());
                    }
                })
                .create();

        MessageId id1 = producer.send("test1".getBytes());
        long publish1 = publishTime.get();

        Thread.sleep(10);
        MessageId id2 = producer.send("test2".getBytes());
        long publish2 = publishTime.get();

        Assert.assertTrue(publish1 < publish2);

        Assert.assertEquals(admin.topics().getMessageIdByTimestamp(topicName, publish1 - 1), id1);
        Assert.assertEquals(admin.topics().getMessageIdByTimestamp(topicName, publish1), id1);
        Assert.assertEquals(admin.topics().getMessageIdByTimestamp(topicName, publish1 + 1), id2);
        Assert.assertEquals(admin.topics().getMessageIdByTimestamp(topicName, publish2), id2);
        Assert.assertTrue(admin.topics().getMessageIdByTimestamp(topicName, publish2 + 1)
                .compareTo(id2) > 0);
    }

    @Test
    public void testGetBatchMessageIdByTimestamp() throws Exception {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("tenant-xyz", tenantInfo);
        admin.namespaces().createNamespace("tenant-xyz/ns-abc", Sets.newHashSet("test"));
        final String topicName = "persistent://tenant-xyz/ns-abc/testGetBatchMessageIdByTimestamp";
        admin.topics().createNonPartitionedTopic(topicName);

        Map<MessageId, Long> publishTimeMap = new ConcurrentHashMap<>();

        ProducerBase<byte[]> producer = (ProducerBase<byte[]>) pulsarClient.newProducer().topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.MINUTES)
                .batchingMaxMessages(2)
                .intercept(new ProducerInterceptor() {
                    @Override
                    public void close() {

                    }

                    @Override
                    public boolean eligible(Message message) {
                        return true;
                    }

                    @Override
                    public Message beforeSend(Producer producer, Message message) {
                        return message;
                    }

                    @Override
                    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId,
                                                      Throwable exception) {
                        log.info("onSendAcknowledgement, message={}, msgId={},publish_time={},exception={}",
                                message, msgId, message.getPublishTime(), exception);
                        publishTimeMap.put(msgId, message.getPublishTime());

                    }
                })
                .create();

        List<CompletableFuture<MessageId>> idFutureList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            idFutureList.add(producer.sendAsync(new byte[]{(byte) i}));
            Thread.sleep(5);
        }

        List<MessageIdImpl> ids = new ArrayList<>();
        for (CompletableFuture<MessageId> future : idFutureList) {
            MessageId id = future.get();
            ids.add((MessageIdImpl) id);
        }

        for (MessageIdImpl messageId : ids) {
            Assert.assertTrue(publishTimeMap.containsKey(messageId));
            log.info("MessageId={},PublishTime={}", messageId, publishTimeMap.get(messageId));
        }

        //message 0, 1 are in the same batch, as batchingMaxMessages is set to 2.
        Assert.assertEquals(ids.get(0).getLedgerId(), ids.get(1).getLedgerId());
        MessageIdImpl id1 =
                new MessageIdImpl(ids.get(0).getLedgerId(), ids.get(0).getEntryId(), ids.get(0).getPartitionIndex());
        long publish1 = publishTimeMap.get(ids.get(0));

        Assert.assertEquals(ids.get(2).getLedgerId(), ids.get(3).getLedgerId());
        MessageIdImpl id2 =
                new MessageIdImpl(ids.get(2).getLedgerId(), ids.get(2).getEntryId(), ids.get(2).getPartitionIndex());
        long publish2 = publishTimeMap.get(ids.get(2));


        Assert.assertTrue(publish1 < publish2);

        Assert.assertEquals(admin.topics().getMessageIdByTimestamp(topicName, publish1 - 1), id1);
        Assert.assertEquals(admin.topics().getMessageIdByTimestamp(topicName, publish1), id1);
        Assert.assertEquals(admin.topics().getMessageIdByTimestamp(topicName, publish1 + 1), id2);
        Assert.assertEquals(admin.topics().getMessageIdByTimestamp(topicName, publish2), id2);
        Assert.assertTrue(admin.topics().getMessageIdByTimestamp(topicName, publish2 + 1)
                .compareTo(id2) > 0);
    }
}
