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
package org.apache.pulsar.broker.transaction.buffer;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Transaction buffer close test.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionBufferCloseTest extends TransactionTestBase {

    @BeforeMethod
    protected void setup() throws Exception {
        setUpBase(1, 16, null, 0);
        Awaitility.await().until(() -> ((PulsarClientImpl) pulsarClient)
                .getTcClient().getState() == TransactionCoordinatorClient.State.READY);
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "isPartition")
    public Object[][] isPartition() {
        return new Object[][]{
                { true }, { false }
        };
    }

    @Test(timeOut = 10_000, dataProvider = "isPartition")
    public void deleteTopicCloseTransactionBufferTest(boolean isPartition) throws Exception {
        int expectedCount = isPartition ? 30 : 1;
        TopicName topicName = createAndLoadTopic(isPartition, expectedCount);
        checkSnapshotPublisherCount(topicName.getNamespace(), expectedCount);
        if (isPartition) {
            admin.topics().deletePartitionedTopic(topicName.getPartitionedTopicName(), true);
        } else {
            admin.topics().delete(topicName.getPartitionedTopicName(), true);
        }
        checkSnapshotPublisherCount(topicName.getNamespace(), 0);
    }

    @Test(timeOut = 10_000, dataProvider = "isPartition")
    public void unloadTopicCloseTransactionBufferTest(boolean isPartition) throws Exception {
        int expectedCount = isPartition ? 30 : 1;
        TopicName topicName = createAndLoadTopic(isPartition, expectedCount);
        checkSnapshotPublisherCount(topicName.getNamespace(), expectedCount);
        admin.topics().unload(topicName.getPartitionedTopicName());
        checkSnapshotPublisherCount(topicName.getNamespace(), 0);
    }

    private TopicName createAndLoadTopic(boolean isPartition, int partitionCount)
            throws PulsarAdminException, PulsarClientException {
        String namespace = TENANT + "/ns-" + RandomStringUtils.randomAlphabetic(5);
        admin.namespaces().createNamespace(namespace, 3);
        String topic = namespace + "/tb-close-test-";
        if (isPartition) {
            admin.topics().createPartitionedTopic(topic, partitionCount);
        }
        pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create()
                .close();
        return TopicName.get(topic);
    }

    private void checkSnapshotPublisherCount(String namespace, int expectCount) throws PulsarAdminException {
        TopicName snTopicName = TopicName.get(TopicDomain.persistent.value(), NamespaceName.get(namespace),
                EventsTopicNames.TRANSACTION_BUFFER_SNAPSHOT);
        List<PublisherStats> publisherStatsList =
                (List<PublisherStats>) admin.topics()
                        .getStats(snTopicName.getPartitionedTopicName()).getPublishers();
        Assert.assertEquals(publisherStatsList.size(), expectCount);
    }

}
