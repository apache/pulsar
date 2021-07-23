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
package org.apache.pulsar.broker.transaction;

import static org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl.TRANSACTION_LOG_PREFIX;
import com.google.common.collect.Sets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar client transaction test.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionTest extends TransactionTestBase {

    private static final String TENANT = "tnx";
    private static final String NAMESPACE1 = TENANT + "/ns1";

    @BeforeMethod
    protected void setup() throws Exception {
        this.setBrokerCount(1);
        this.internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length - 1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 1);
        pulsarClient.close();
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
    }

    @Test
    public void brokerNotInitTxnManagedLedgerTopic() throws Exception {
        String subName = "test";

        String topicName = TopicName.get(NAMESPACE1 + "/test").toString();


        @Cleanup
        Consumer<byte[]> consumer = getConsumer(topicName, subName);

        consumer.close();

        Awaitility.await().until(() -> {
            try {
                pulsarClient.newTransaction()
                        .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
            } catch (Exception e) {
                return false;
            }
            return true;
        });

        admin.namespaces().unload(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.namespaces().unload(NAMESPACE1);

        @Cleanup
        Consumer<byte[]> consumer1 = getConsumer(topicName, subName);

        Awaitility.await().until(() -> {
            try {
                pulsarClient.newTransaction()
                        .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
            } catch (Exception e) {
                return false;
            }
            return true;
        });

        ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                getPulsarServiceList().get(0).getBrokerService().getTopics();

        Assert.assertNull(topics.get(TopicName.get(TopicDomain.persistent.value(),
                NamespaceName.SYSTEM_NAMESPACE, TRANSACTION_LOG_PREFIX).toString() + 0));
        Assert.assertNull(topics.get(TopicName.TRANSACTION_COORDINATOR_ASSIGN.getPartition(0).toString()));
        Assert.assertNull(topics.get(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topicName, subName)));
    }


    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public Consumer<byte[]> getConsumer(String topicName, String subName) throws PulsarClientException {
        return pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();
    }
}