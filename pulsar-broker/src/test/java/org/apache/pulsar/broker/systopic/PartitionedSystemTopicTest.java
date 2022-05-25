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
package org.apache.pulsar.broker.systopic;

import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pulsar.broker.admin.impl.BrokersBase;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.ListTopicsOptions;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker")
public class PartitionedSystemTopicTest extends BrokerTestBase {

    static final int PARTITIONS = 5;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        resetConfig();
        conf.setAllowAutoTopicCreation(false);
        conf.setAllowAutoTopicCreationType("partitioned");
        conf.setDefaultNumPartitions(PARTITIONS);

        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAutoCreatedPartitionedSystemTopic() throws Exception {
        final String ns = "prop/ns-test";
        admin.namespaces().createNamespace(ns, 2);
        NamespaceEventsSystemTopicFactory systemTopicFactory = new NamespaceEventsSystemTopicFactory(pulsarClient);
        TopicPoliciesSystemTopicClient systemTopicClientForNamespace = systemTopicFactory
                .createTopicPoliciesSystemTopicClient(NamespaceName.get(ns));
        SystemTopicClient.Reader reader = systemTopicClientForNamespace.newReader();

        int partitions = admin.topics().getPartitionedTopicMetadata(
                String.format("persistent://%s/%s", ns, SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME)).partitions;
        List<String> partitionedTopicList = admin.topics().getPartitionedTopicList(ns,
                ListTopicsOptions.builder().includeSystemTopic(true).build());
        Assert.assertEquals(partitionedTopicList.size(), 1);
        Assert.assertEquals(partitions, PARTITIONS);
        Assert.assertEquals(admin.topics().getList(ns, null,
                ListTopicsOptions.builder().includeSystemTopic(true).build()).size(), PARTITIONS);
        reader.close();
    }

    @Test(timeOut = 1000 * 60)
    public void testConsumerCreationWhenEnablingTopicPolicy() throws Exception {
        String tenant = "tenant-" + RandomStringUtils.randomAlphabetic(4).toLowerCase();
        admin.tenants().createTenant(tenant, new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet("test")));
        int namespaceCount = 30;
        for (int i = 0; i < namespaceCount; i++) {
            String ns = tenant + "/ns-" + i;
            admin.namespaces().createNamespace(ns, 4);
            String topic = ns + "/t1";
            admin.topics().createPartitionedTopic(topic, 2);
        }

        List<CompletableFuture<Consumer<byte[]>>> futureList = new ArrayList<>();
        for (int i = 0; i < namespaceCount; i++) {
            String topic = tenant + "/ns-" + i + "/t1";
            futureList.add(pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionName("sub")
                    .subscribeAsync());
        }
        FutureUtil.waitForAll(futureList).get();
        // Close all the consumers after check
        for (CompletableFuture<Consumer<byte[]>> consumer : futureList) {
            consumer.join().close();
        }
    }

    @Test
    public void testProduceAndConsumeUnderSystemNamespace() throws Exception {
        TenantInfo tenantInfo = TenantInfo
                .builder()
                .adminRoles(Sets.newHashSet("admin"))
                .allowedClusters(Sets.newHashSet("test"))
                .build();
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", 2);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic("pulsar/system/__topic-1").create();
        producer.send("test".getBytes(StandardCharsets.UTF_8));
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic("pulsar/system/__topic-1")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("sub1")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        Message<byte[]> receive = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNotNull(receive);
    }

    @Test
    public void testHealthCheckTopicNotOffload() throws Exception {
        NamespaceName namespaceName = NamespaceService.getHeartbeatNamespaceV2(pulsar.getAdvertisedAddress(),
                pulsar.getConfig());
        TopicName topicName = TopicName.get("persistent", namespaceName, BrokersBase.HEALTH_CHECK_TOPIC_SUFFIX);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName.toString(), true).get().get();
        ManagedLedgerConfig config = persistentTopic.getManagedLedger().getConfig();
        config.setLedgerOffloader(NullLedgerOffloader.INSTANCE);
        admin.brokers().healthcheck(TopicVersion.V2);
        admin.topics().triggerOffload(topicName.toString(), MessageId.earliest);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(persistentTopic.getManagedLedger().getOffloadedSize(), 0);
        });
        LedgerOffloader ledgerOffloader = Mockito.mock(LedgerOffloader.class);
        config.setLedgerOffloader(ledgerOffloader);
        Assert.assertEquals(config.getLedgerOffloader(), ledgerOffloader);
        admin.topicPolicies().setMaxConsumers(topicName.toString(), 2);
        Awaitility.await().pollDelay(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertEquals(persistentTopic.getManagedLedger().getConfig().getLedgerOffloader(),
                    NullLedgerOffloader.INSTANCE);
        });
    }

}
