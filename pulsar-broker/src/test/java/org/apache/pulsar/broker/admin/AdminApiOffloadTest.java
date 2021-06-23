
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AdminApiOffloadTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "prop-xyz";
    private final String testNamespace = "ns1";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/test-";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setManagedLedgerMaxEntriesPerLedger(10);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);

        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(testTenant, tenantInfo);
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private void testOffload(String topicName, String mlName) throws Exception {
        LedgerOffloader offloader = mock(LedgerOffloader.class);
        when(offloader.getOffloadDriverName()).thenReturn("mock");

        doReturn(offloader).when(pulsar).getManagedLedgerOffloader(any(), any());

        CompletableFuture<Void> promise = new CompletableFuture<>();
        doReturn(promise).when(offloader).offload(any(), any(), any());

        MessageId currentId = MessageId.latest;
        try (Producer<byte[]> p = pulsarClient.newProducer().topic(topicName).enableBatching(false).create()) {
            for (int i = 0; i < 15; i++) {
                currentId = p.send("Foobar".getBytes());
            }
        }

        ManagedLedgerInfo info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(mlName);
        assertEquals(info.ledgers.size(), 2);

        assertEquals(admin.topics().offloadStatus(topicName).getStatus(),
                            LongRunningProcessStatus.Status.NOT_RUN);

        admin.topics().triggerOffload(topicName, currentId);

        assertEquals(admin.topics().offloadStatus(topicName).getStatus(),
                            LongRunningProcessStatus.Status.RUNNING);

        try {
            admin.topics().triggerOffload(topicName, currentId);
            Assert.fail("Should have failed");
        } catch (ConflictException e) {
            // expected
        }

        // fail first time
        promise.completeExceptionally(new Exception("Some random failure"));

        assertEquals(admin.topics().offloadStatus(topicName).getStatus(),
                            LongRunningProcessStatus.Status.ERROR);
        Assert.assertTrue(admin.topics().offloadStatus(topicName).getLastError().contains("Some random failure"));

        // Try again
        doReturn(CompletableFuture.completedFuture(null))
            .when(offloader).offload(any(), any(), any());

        admin.topics().triggerOffload(topicName, currentId);

        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.topics().offloadStatus(topicName).getStatus(),
                LongRunningProcessStatus.Status.SUCCESS));
        MessageId firstUnoffloaded = admin.topics().offloadStatus(topicName).getFirstUnoffloadedMessage();
        assertTrue(firstUnoffloaded instanceof MessageIdImpl);
        MessageIdImpl firstUnoffloadedMessage = (MessageIdImpl) firstUnoffloaded;
        // First unoffloaded is the first entry of current ledger
        assertEquals(firstUnoffloadedMessage.getLedgerId(), info.ledgers.get(1).ledgerId);
        assertEquals(firstUnoffloadedMessage.getEntryId(), 0);

        verify(offloader, times(2)).offload(any(), any(), any());
    }


    @Test
    public void testOffloadV2() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/topic1";
        String mlName = "prop-xyz/ns1/persistent/topic1";
        testOffload(topicName, mlName);
    }

    @Test
    public void testOffloadV1() throws Exception {
        String topicName = "persistent://prop-xyz/test/ns1/topic2";
        String mlName = "prop-xyz/test/ns1/persistent/topic2";
        testOffload(topicName, mlName);
    }

    @Test
    public void testOffloadPolicies() throws Exception {
        String namespaceName = "prop-xyz/ns1";
        String driver = "aws-s3";
        String region = "test-region";
        String bucket = "test-bucket";
        String endpoint = "test-endpoint";
        long offloadThresholdInBytes = 0;
        long offloadDeletionLagInMillis = 100L;
        OffloadedReadPriority priority = OffloadedReadPriority.TIERED_STORAGE_FIRST;

        OffloadPolicies offload1 = OffloadPoliciesImpl.create(
                driver, region, bucket, endpoint, null, null, null, null,
                100, 100, offloadThresholdInBytes, offloadDeletionLagInMillis, priority);
        admin.namespaces().setOffloadPolicies(namespaceName, offload1);
        OffloadPolicies offload2 = admin.namespaces().getOffloadPolicies(namespaceName);
        assertEquals(offload1, offload2);

        admin.namespaces().removeOffloadPolicies(namespaceName);
        OffloadPolicies offload3 = admin.namespaces().getOffloadPolicies(namespaceName);
        assertNull(offload3);
    }

    @Test
    public void testOffloadPoliciesApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        pulsarClient.newProducer().topic(topicName).create().close();
        OffloadPoliciesImpl offloadPolicies = (OffloadPoliciesImpl) admin.topics().getOffloadPolicies(topicName);
        assertNull(offloadPolicies);
        OffloadPoliciesImpl offload = new OffloadPoliciesImpl();
        String path = "fileSystemPath";
        offload.setFileSystemProfilePath(path);
        admin.topics().setOffloadPolicies(topicName, offload);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getOffloadPolicies(topicName)));
        assertEquals(admin.topics().getOffloadPolicies(topicName), offload);
        assertEquals(admin.topics().getOffloadPolicies(topicName).getFileSystemProfilePath(), path);
        admin.topics().removeOffloadPolicies(topicName);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topics().getOffloadPolicies(topicName)));
        assertNull(admin.topics().getOffloadPolicies(topicName));
    }

    @Test
    public void testOffloadPoliciesAppliedApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, 3);
        pulsarClient.newProducer().topic(topicName).create().close();
        OffloadPoliciesImpl offloadPolicies = (OffloadPoliciesImpl) admin.topics().getOffloadPolicies(topicName, true);
        OffloadPoliciesImpl brokerPolicies = OffloadPoliciesImpl
                .mergeConfiguration(null,null, pulsar.getConfiguration().getProperties());

        assertEquals(offloadPolicies, brokerPolicies);
        //Since off loader is not really set, avoid code exceptions
        LedgerOffloader topicOffloaded = mock(LedgerOffloader.class);
        when(topicOffloaded.getOffloadDriverName()).thenReturn("mock");
        doReturn(topicOffloaded).when(pulsar).createManagedLedgerOffloader(any());

        OffloadPoliciesImpl namespacePolicies = new OffloadPoliciesImpl();
        namespacePolicies.setManagedLedgerOffloadThresholdInBytes(100L);
        namespacePolicies.setManagedLedgerOffloadDeletionLagInMillis(200L);
        namespacePolicies.setManagedLedgerOffloadDriver("s3");
        namespacePolicies.setManagedLedgerOffloadBucket("buck");
        admin.namespaces().setOffloadPolicies(myNamespace, namespacePolicies);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.namespaces().getOffloadPolicies(myNamespace), namespacePolicies));
        assertEquals(
                admin.topics().getOffloadPolicies(topicName, true), namespacePolicies);

        OffloadPoliciesImpl topicPolicies = new OffloadPoliciesImpl();
        topicPolicies.setManagedLedgerOffloadThresholdInBytes(200L);
        topicPolicies.setManagedLedgerOffloadDeletionLagInMillis(400L);
        topicPolicies.setManagedLedgerOffloadDriver("s3");
        topicPolicies.setManagedLedgerOffloadBucket("buck2");

        admin.topics().setOffloadPolicies(topicName, topicPolicies);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getOffloadPolicies(topicName, true), topicPolicies));

        admin.topics().removeOffloadPolicies(topicName);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getOffloadPolicies(topicName, true), namespacePolicies));

        admin.namespaces().removeOffloadPolicies(myNamespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getOffloadPolicies(topicName, true), brokerPolicies));
    }

    @Test
    public void testTopicLevelOffloadPartitioned() throws Exception {
        testOffload(true);
    }

    @Test
    public void testTopicLevelOffloadNonPartitioned() throws Exception {
        testOffload(false);
    }

    private void testOffload(boolean isPartitioned) throws Exception {
        String topicName = testTopic + UUID.randomUUID().toString();
        int partitionNum = 3;
        //1 create topic
        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topicName, partitionNum);
        } else {
            admin.topics().createNonPartitionedTopic(topicName);
        }
        pulsarClient.newProducer().topic(topicName).enableBatching(false).create().close();
        //2 namespace level policy should use NullLedgerOffloader by default
        if (isPartitioned) {
            for (int i = 0; i < partitionNum; i++) {
                PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopicIfExists(TopicName.get(topicName).getPartition(i).toString()).get().get();
                assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
                assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                        , "NullLedgerOffloader");
            }
        } else {
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).get().get();
            assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
            assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                    , "NullLedgerOffloader");
        }
        //3 construct a topic level offloadPolicies
        OffloadPoliciesImpl offloadPolicies = new OffloadPoliciesImpl();
        offloadPolicies.setOffloadersDirectory(".");
        offloadPolicies.setManagedLedgerOffloadDriver("mock");
        offloadPolicies.setManagedLedgerOffloadPrefetchRounds(10);
        offloadPolicies.setManagedLedgerOffloadThresholdInBytes(1024L);

        LedgerOffloader topicOffloader = mock(LedgerOffloader.class);
        when(topicOffloader.getOffloadDriverName()).thenReturn("mock");
        doReturn(topicOffloader).when(pulsar).createManagedLedgerOffloader(any());

        //4 set topic level offload policies
        admin.topics().setOffloadPolicies(topicName, offloadPolicies);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getOffloadPolicies(topicName)));
        //5 name of offload should become "mock"
        if (isPartitioned) {
            for (int i = 0; i < partitionNum; i++) {
                PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopic(TopicName.get(topicName).getPartition(i).toString(), false).get().get();
                assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
                assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                        , "mock");
            }
        } else {
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).get().get();
            assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
            assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                    , "mock");
        }
        //6 remove topic level offload policy, offloader should become namespaceOffloader
        LedgerOffloader namespaceOffloader = mock(LedgerOffloader.class);
        when(namespaceOffloader.getOffloadDriverName()).thenReturn("s3");
        Map<NamespaceName, LedgerOffloader> map = new HashMap<>();
        map.put(TopicName.get(topicName).getNamespaceObject(), namespaceOffloader);
        doReturn(map).when(pulsar).getLedgerOffloaderMap();
        doReturn(namespaceOffloader).when(pulsar)
                .getManagedLedgerOffloader(TopicName.get(topicName).getNamespaceObject(), null);

        admin.topics().removeOffloadPolicies(topicName);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topics().getOffloadPolicies(topicName)));
        // topic level offloader should be closed
        if (isPartitioned) {
            verify(topicOffloader, times(partitionNum)).close();
        } else {
            verify(topicOffloader).close();
        }
        if (isPartitioned) {
            for (int i = 0; i < partitionNum; i++) {
                PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopicIfExists(TopicName.get(topicName).getPartition(i).toString()).get().get();
                assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
                assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                        , "s3");
            }
        } else {
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).get().get();
            assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
            assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                    , "s3");
        }
    }

}
