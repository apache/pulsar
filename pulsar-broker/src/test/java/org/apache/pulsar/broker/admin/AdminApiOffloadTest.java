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

import static org.assertj.core.api.Assertions.assertThat;
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
import static org.testng.Assert.fail;
import io.opentelemetry.api.common.Attributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
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
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
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

        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant(testTenant, tenantInfo);
        admin.namespaces().createNamespace(myNamespace, Set.of("test"));
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private void testOffloadWithMessageId(String topicName, String mlName) throws Exception {
        LedgerOffloader offloader = mock(LedgerOffloader.class);
        when(offloader.getOffloadDriverName()).thenReturn("mock");

        doReturn(offloader).when(pulsar).getManagedLedgerOffloader(any(), any());

        CompletableFuture<Void> promise = new CompletableFuture<>();
        doReturn(promise).when(offloader).offload(any(), any(), any());
        doReturn(true).when(offloader).isAppendable();

        MessageId currentId = MessageId.latest;
        try (Producer<byte[]> p = pulsarClient.newProducer().topic(topicName).enableBatching(false).create()) {
            for (int i = 0; i < 15; i++) {
                currentId = p.send("Foobar".getBytes());
            }
        }

        ManagedLedgerInfo info = pulsar.getDefaultManagedLedgerFactory().getManagedLedgerInfo(mlName);
        assertEquals(info.ledgers.size(), 2);

        assertEquals(admin.topics().offloadStatus(topicName).getStatus(), LongRunningProcessStatus.Status.NOT_RUN);
        var topicNameObject = TopicName.get(topicName);
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, topicNameObject.getDomain().toString())
                .put(OpenTelemetryAttributes.PULSAR_TENANT, topicNameObject.getTenant())
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, topicNameObject.getNamespace())
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicNameObject.getPartitionedTopicName())
                .build();
        // Verify the respective metric is 0 before the offload begins.
        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        BrokerOpenTelemetryTestUtil.assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OFFLOADED_COUNTER,
                attributes, actual -> assertThat(actual).isZero());

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

        // Verify the metrics have been updated.
        metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        BrokerOpenTelemetryTestUtil.assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OFFLOADED_COUNTER,
                attributes, actual -> assertThat(actual).isPositive());
    }

    private void testOffloadWithSizeThreshold(String topicName, String mlName) throws Exception {
        LedgerOffloader offloader = mock(LedgerOffloader.class);
        when(offloader.getOffloadDriverName()).thenReturn("mock");

        doReturn(offloader).when(pulsar).getManagedLedgerOffloader(any(), any());

        CompletableFuture<Void> promise = new CompletableFuture<>();
        doReturn(promise).when(offloader).offload(any(), any(), any());
        doReturn(true).when(offloader).isAppendable();

        try (Producer<byte[]> p = pulsarClient.newProducer().topic(topicName).enableBatching(false).create()) {
            for (int i = 0; i < 15; i++) {
                p.send("Foobar".getBytes());
            }
        }

        ManagedLedgerInfo info = pulsar.getDefaultManagedLedgerFactory().getManagedLedgerInfo(mlName);
        assertEquals(info.ledgers.size(), 2);

        assertEquals(admin.topics().offloadStatus(topicName).getStatus(), LongRunningProcessStatus.Status.NOT_RUN);
        var topicNameObject = TopicName.get(topicName);
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, topicNameObject.getDomain().toString())
                .put(OpenTelemetryAttributes.PULSAR_TENANT, topicNameObject.getTenant())
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, topicNameObject.getNamespace())
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicNameObject.getPartitionedTopicName())
                .build();
        // Verify the respective metric is 0 before the offload begins.
        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        BrokerOpenTelemetryTestUtil.assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OFFLOADED_COUNTER,
                attributes, actual -> assertThat(actual).isZero());

        admin.topics().triggerOffload(topicName, 50L);

        assertEquals(admin.topics().offloadStatus(topicName).getStatus(),
                LongRunningProcessStatus.Status.RUNNING);

        try {
            admin.topics().triggerOffload(topicName, 50L);
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

        admin.topics().triggerOffload(topicName, 30L);

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

        // Verify the metrics have been updated.
        metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        BrokerOpenTelemetryTestUtil.assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OFFLOADED_COUNTER,
                attributes, actual -> assertThat(actual).isPositive());
    }

    @Test
    public void testOffloadWithMessageIdV2() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/topic1";
        String mlName = "prop-xyz/ns1/persistent/topic1";
        testOffloadWithMessageId(topicName, mlName);
    }

    @Test
    public void testOffloadWithSizeThresholdV2() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/topic1";
        String mlName = "prop-xyz/ns1/persistent/topic1";
        testOffloadWithSizeThreshold(topicName, mlName);
    }

    @Test
    public void testOffloadWithMessageIdV1() throws Exception {
        String topicName = "persistent://prop-xyz/test/ns1/topic2";
        String mlName = "prop-xyz/test/ns1/persistent/topic2";
        testOffloadWithMessageId(topicName, mlName);
    }

    @Test
    public void testOffloadWithSizeThresholdV1() throws Exception {
        String topicName = "persistent://prop-xyz/test/ns1/topic2";
        String mlName = "prop-xyz/test/ns1/persistent/topic2";
        testOffloadWithSizeThreshold(topicName, mlName);
    }

    @Test
    public void testOffloadPolicies() throws Exception {
        String namespaceName = "prop-xyz/ns1";
        String driver = "aws-s3";
        String region = "test-region";
        String bucket = "test-bucket";
        String endpoint = "test-endpoint";
        long offloadThresholdInBytes = 0;
        long offloadThresholdInSeconds = 100;
        long offloadDeletionLagInMillis = 100L;
        OffloadedReadPriority priority = OffloadedReadPriority.TIERED_STORAGE_FIRST;

        OffloadPolicies offload1 = OffloadPoliciesImpl.create(
                driver, region, bucket, endpoint, null, null, null, null,
                100, 100, offloadThresholdInBytes, offloadThresholdInSeconds, offloadDeletionLagInMillis, priority);
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
        offload.setManagedLedgerOffloadDriver("S3");
        offload.setManagedLedgerOffloadBucket("bucket");
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
        namespacePolicies.setManagedLedgerOffloadThresholdInSeconds(100L);
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
        topicPolicies.setManagedLedgerOffloadThresholdInSeconds(200L);
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
    public void testSetNamespaceOffloadPolicies() throws Exception {
        conf.setManagedLedgerOffloadThresholdInSeconds(100);
        conf.setManagedLedgerOffloadAutoTriggerSizeThresholdBytes(100);

        OffloadPoliciesImpl policies = new OffloadPoliciesImpl();
        policies.setManagedLedgerOffloadThresholdInBytes(200L);
        policies.setManagedLedgerOffloadThresholdInSeconds(200L);
        policies.setManagedLedgerOffloadDeletionLagInMillis(400L);
        policies.setManagedLedgerOffloadDriver("s3");
        policies.setManagedLedgerOffloadBucket("buck2");

        admin.namespaces().setOffloadThresholdInSeconds(myNamespace, 300);
        assertEquals(admin.namespaces().getOffloadThresholdInSeconds(myNamespace), 300);

        admin.namespaces().setOffloadPolicies(myNamespace, policies);
        assertEquals(admin.namespaces().getOffloadPolicies(myNamespace), policies);
    }

    @Test
    public void testSetNamespaceOffloadPoliciesFailByReadOnly() throws Exception {
        boolean setNsPolicyReadOnlySuccess = false;
        try {
            pulsar.getConfigurationMetadataStore().put(NamespaceResources.POLICIES_READONLY_FLAG_PATH, "0".getBytes(),
                    Optional.empty()).join();
            setNsPolicyReadOnlySuccess = true;
            admin.namespaces().setOffloadThresholdInSeconds(myNamespace, 300);
            fail("set offload threshold should fail when ns policies is readonly");
        } catch (Exception ex){
            // ignore.
        } finally {
            // cleanup.
            if (setNsPolicyReadOnlySuccess) {
                pulsar.getConfigurationMetadataStore().delete(NamespaceResources.POLICIES_READONLY_FLAG_PATH,
                        Optional.empty()).join();
            }
        }
    }

    @Test
    public void testSetTopicOffloadPolicies() throws Exception {
        conf.setManagedLedgerOffloadThresholdInSeconds(100);
        conf.setManagedLedgerOffloadAutoTriggerSizeThresholdBytes(100);

        LedgerOffloader topicOffloader = mock(LedgerOffloader.class);
        when(topicOffloader.getOffloadDriverName()).thenReturn("mock");
        doReturn(topicOffloader).when(pulsar).createManagedLedgerOffloader(any());

        OffloadPoliciesImpl namespacePolicies = new OffloadPoliciesImpl();
        namespacePolicies.setManagedLedgerOffloadThresholdInBytes(200L);
        namespacePolicies.setManagedLedgerOffloadThresholdInSeconds(200L);
        namespacePolicies.setManagedLedgerOffloadDeletionLagInMillis(400L);
        namespacePolicies.setManagedLedgerOffloadDriver("s3");
        namespacePolicies.setManagedLedgerOffloadBucket("buck2");

        admin.namespaces().setOffloadThresholdInSeconds(myNamespace, 300);
        assertEquals(admin.namespaces().getOffloadThresholdInSeconds(myNamespace), 300);

        admin.namespaces().setOffloadPolicies(myNamespace, namespacePolicies);
        assertEquals(admin.namespaces().getOffloadPolicies(myNamespace), namespacePolicies);

        OffloadPoliciesImpl topicPolicies = new OffloadPoliciesImpl();
        topicPolicies.setManagedLedgerOffloadThresholdInBytes(500L);
        topicPolicies.setManagedLedgerOffloadThresholdInSeconds(500L);
        topicPolicies.setManagedLedgerOffloadDeletionLagInMillis(400L);
        topicPolicies.setManagedLedgerOffloadDriver("s3");
        topicPolicies.setManagedLedgerOffloadBucket("buck2");

        String topicName = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topicPolicies().setOffloadPolicies(topicName, topicPolicies);

        // Wait until broker update policies finished.
        for (int a = 1; a <= 5; a++) {
            try {
                OffloadPolicies policies = admin.topicPolicies().getOffloadPolicies(topicName);

                assertEquals(policies.getManagedLedgerOffloadThresholdInSeconds(),
                        topicPolicies.getManagedLedgerOffloadThresholdInSeconds());
                assertEquals(policies.getManagedLedgerOffloadThresholdInBytes(),
                        topicPolicies.getManagedLedgerOffloadThresholdInBytes());
            } catch (Exception e) {
                if (a == 5) {
                    throw e;
                } else {
                    Thread.sleep(1000L);
                }
            }
        }
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
        offloadPolicies.setManagedLedgerOffloadDriver("S3");
        offloadPolicies.setManagedLedgerOffloadBucket("bucket");
        offloadPolicies.setManagedLedgerOffloadPrefetchRounds(10);
        offloadPolicies.setManagedLedgerOffloadThresholdInBytes(1024L);

        LedgerOffloader topicOffloader = mock(LedgerOffloader.class);
        when(topicOffloader.getOffloadDriverName()).thenReturn("S3");
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
                        , "S3");
            }
        } else {
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).get().get();
            assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
            assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                    , "S3");
        }
        //6 remove topic level offload policy, offloader should become namespaceOffloader
        LedgerOffloader namespaceOffloader = mock(LedgerOffloader.class);
        when(namespaceOffloader.getOffloadDriverName()).thenReturn("S3");
        Map<NamespaceName, LedgerOffloader> map = new HashMap<>();
        map.put(TopicName.get(topicName).getNamespaceObject(), namespaceOffloader);
        doReturn(map).when(pulsar).getLedgerOffloaderMap();
        doReturn(namespaceOffloader).when(pulsar)
                .getManagedLedgerOffloader(TopicName.get(topicName).getNamespaceObject(), null);

        admin.topics().removeOffloadPolicies(topicName);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topics().getOffloadPolicies(topicName)));
        if (isPartitioned) {
            for (int i = 0; i < partitionNum; i++) {
                PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopicIfExists(TopicName.get(topicName).getPartition(i).toString()).get().get();
                assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
                assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                        , "S3");
            }
        } else {
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).get().get();
            assertNotNull(topic.getManagedLedger().getConfig().getLedgerOffloader());
            assertEquals(topic.getManagedLedger().getConfig().getLedgerOffloader().getOffloadDriverName()
                    , "S3");
        }
    }

}
