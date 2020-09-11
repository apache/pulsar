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
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferClientImpl;
import org.apache.pulsar.broker.transaction.coordinator.TransactionMetaStoreTestBase;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TransactionBufferClientTest extends TransactionMetaStoreTestBase {

    private static final Logger log = LoggerFactory.getLogger(TransactionBufferClientTest.class);
    private TransactionBufferClient tbClient;
    TopicName partitionedTopicName = TopicName.get("persistent", "public", "test", "tb-client");
    int partitions = 10;

    @BeforeClass
    void init() throws Exception {
        super.setup();
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        pulsarAdmins[0].tenants().createTenant("public", new TenantInfo(Sets.newHashSet(), Sets.newHashSet("my-cluster")));
        pulsarAdmins[0].namespaces().createNamespace("public/test", 10);
        pulsarAdmins[0].topics().createPartitionedTopic(partitionedTopicName.getPartitionedTopicName(), partitions);
        pulsarClient.newConsumer()
                .topic(partitionedTopicName.getPartitionedTopicName())
                .subscriptionName("test").subscribe();
        tbClient = TransactionBufferClientImpl.create(pulsarServices[0].getNamespaceService(),
                ((PulsarClientImpl) pulsarClient).getCnxPool());
    }

    @Override
    public void afterPulsarStart() throws Exception {
        super.afterPulsarStart();
        for (int i = 0; i < pulsarServices.length; i++) {
            Subscription mockSubscription = Mockito.mock(Subscription.class);
            Mockito.when(mockSubscription.endTxn(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            Topic mockTopic = Mockito.mock(Topic.class);
            Mockito.when(mockTopic.endTxn(Mockito.any(), Mockito.anyInt()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            Mockito.when(mockTopic.getSubscription(Mockito.any())).thenReturn(mockSubscription);

            ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topicMap =
                    Mockito.mock(ConcurrentOpenHashMap.class);
            Mockito.when(topicMap.get(Mockito.anyString())).thenReturn(
                    CompletableFuture.completedFuture(Optional.of(mockTopic)));

            BrokerService brokerService = Mockito.spy(new BrokerService(pulsarServices[i]));
            Mockito.when(brokerService.getTopics()).thenReturn(topicMap);
            Mockito.when(pulsarServices[i].getBrokerService()).thenReturn(brokerService);
        }
    }

    @Test
    public void testCommitOnTopic() throws ExecutionException, InterruptedException {
        List<CompletableFuture<TxnID>> futures = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            futures.add(tbClient.commitTxnOnTopic(topic, 1L, i));
        }
        for (int i = 0; i < futures.size(); i++) {
            Assert.assertEquals(futures.get(i).get().getMostSigBits(), 1L);
            Assert.assertEquals(futures.get(i).get().getLeastSigBits(), i);
        }
    }

    @Test
    public void testAbortOnTopic() throws ExecutionException, InterruptedException {
        List<CompletableFuture<TxnID>> futures = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            futures.add(tbClient.abortTxnOnTopic(topic, 1L, i));
        }
        for (int i = 0; i < futures.size(); i++) {
            Assert.assertEquals(futures.get(i).get().getMostSigBits(), 1L);
            Assert.assertEquals(futures.get(i).get().getLeastSigBits(), i);
        }
    }

    @Test
    public void testCommitOnSubscription() throws ExecutionException, InterruptedException {
        List<CompletableFuture<TxnID>> futures = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            futures.add(tbClient.commitTxnOnSubscription(topic, "test", 1L, i));
        }
        for (int i = 0; i < futures.size(); i++) {
            Assert.assertEquals(futures.get(i).get().getMostSigBits(), 1L);
            Assert.assertEquals(futures.get(i).get().getLeastSigBits(), i);
        }
    }

    @Test
    public void testAbortOnSubscription() throws ExecutionException, InterruptedException {
        List<CompletableFuture<TxnID>> futures = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            futures.add(tbClient.abortTxnOnSubscription(topic, "test", 1L, i));
        }
        for (int i = 0; i < futures.size(); i++) {
            Assert.assertEquals(futures.get(i).get().getMostSigBits(), 1L);
            Assert.assertEquals(futures.get(i).get().getLeastSigBits(), i);
        }
    }
}
