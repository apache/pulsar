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
package org.apache.pulsar.broker.transaction.buffer;

import static org.mockito.ArgumentMatchers.anyString;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Cleanup;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.PrometheusMetricsTest;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferClientImpl;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferHandlerImpl;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "broker")
public class TransactionBufferClientTest extends TransactionTestBase {

    private static final Logger log = LoggerFactory.getLogger(TransactionBufferClientTest.class);
    private TransactionBufferClient tbClient;
    TopicName partitionedTopicName = TopicName.get("persistent", "public", "test", "tb-client");
    int partitions = 10;
    private static final String namespace = "public/test";

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        setBrokerCount(3);
        internalSetup();
        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(namespace, 10);
        admin.topics().createPartitionedTopic(partitionedTopicName.getPartitionedTopicName(), partitions);
        tbClient = TransactionBufferClientImpl.create(pulsarServiceList.get(0),
                new HashedWheelTimer(new DefaultThreadFactory("transaction-buffer")), 1000, 3000);
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        tbClient.close();
        super.internalCleanup();
    }

    @Test
    public void testCommitOnTopic() throws ExecutionException, InterruptedException {
        List<CompletableFuture<TxnID>> futures = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            futures.add(tbClient.commitTxnOnTopic(topic, 1L, i, Long.MIN_VALUE));
        }
        for (int i = 0; i < futures.size(); i++) {
            assertEquals(futures.get(i).get().getMostSigBits(), 1L);
            assertEquals(futures.get(i).get().getLeastSigBits(), i);
        }
    }

    @Test
    public void testAbortOnTopic() throws ExecutionException, InterruptedException {
        List<CompletableFuture<TxnID>> futures = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            futures.add(tbClient.abortTxnOnTopic(topic, 1L, i, Long.MIN_VALUE));
        }
        for (int i = 0; i < futures.size(); i++) {
            assertEquals(futures.get(i).get().getMostSigBits(), 1L);
            assertEquals(futures.get(i).get().getLeastSigBits(), i);
        }
    }

    @Test
    public void testCommitOnSubscription() throws ExecutionException, InterruptedException {
        List<CompletableFuture<TxnID>> futures = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            futures.add(tbClient.commitTxnOnSubscription(topic, "test", 1L, i, -1L));
        }
        for (int i = 0; i < futures.size(); i++) {
            assertEquals(futures.get(i).get().getMostSigBits(), 1L);
            assertEquals(futures.get(i).get().getLeastSigBits(), i);
        }
    }

    @Test
    public void testAbortOnSubscription() throws ExecutionException, InterruptedException {
        List<CompletableFuture<TxnID>> futures = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            futures.add(tbClient.abortTxnOnSubscription(topic, "test", 1L, i, -1L));
        }
        for (int i = 0; i < futures.size(); i++) {
            assertEquals(futures.get(i).get().getMostSigBits(), 1L);
            assertEquals(futures.get(i).get().getLeastSigBits(), i);
        }
    }


    @Test
    public void testTransactionBufferMetrics() throws Exception {
        //Test commit
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            tbClient.commitTxnOnSubscription(topic, "test", 1L, i, -1L).get();
        }

        //test abort
        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            tbClient.abortTxnOnSubscription(topic, "test", 1L, i, -1L).get();
        }

        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsarServiceList.get(0), true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, PrometheusMetricsTest.Metric> metricsMap = PrometheusMetricsTest.parseMetrics(metricsStr);

        Collection<PrometheusMetricsTest.Metric> abortFailed = metricsMap.get("pulsar_txn_tb_client_abort_failed_total");
        Collection<PrometheusMetricsTest.Metric> commitFailed = metricsMap.get("pulsar_txn_tb_client_commit_failed_total");
        Collection<PrometheusMetricsTest.Metric> abortLatencyCount =
                metricsMap.get("pulsar_txn_tb_client_abort_latency_count");
        Collection<PrometheusMetricsTest.Metric> commitLatencyCount =
                metricsMap.get("pulsar_txn_tb_client_commit_latency_count");
        Collection<PrometheusMetricsTest.Metric> pending = metricsMap.get("pulsar_txn_tb_client_pending_requests");

        assertEquals(abortFailed.stream().mapToDouble(metric -> metric.value).sum(), 0);
        assertEquals(commitFailed.stream().mapToDouble(metric -> metric.value).sum(), 0);

        for (int i = 0; i < partitions; i++) {
            String topic = partitionedTopicName.getPartition(i).toString();
            Optional<PrometheusMetricsTest.Metric> optional = abortLatencyCount.stream()
                    .filter(metric -> metric.tags.get("topic").equals(topic)).findFirst();

            assertTrue(optional.isPresent());
            assertEquals(optional.get().value, 1D);

            Optional<PrometheusMetricsTest.Metric> optional1 = commitLatencyCount.stream()
                    .filter(metric -> metric.tags.get("topic").equals(topic)).findFirst();
            assertTrue(optional1.isPresent());
            assertEquals(optional1.get().value, 1D);
        }

        assertEquals(pending.size(), 1);
    }

    @Test
    public void testTransactionBufferClientTimeout() throws Exception {
        PulsarService pulsarService = pulsarServiceList.get(0);
        PulsarClient mockClient = mock(PulsarClientImpl.class);
        CompletableFuture<ClientCnx> completableFuture = new CompletableFuture<>();
        ClientCnx clientCnx = mock(ClientCnx.class);
        completableFuture.complete(clientCnx);
        when(((PulsarClientImpl)mockClient).getConnection(anyString())).thenReturn(completableFuture);
        ChannelHandlerContext cnx = mock(ChannelHandlerContext.class);
        when(clientCnx.ctx()).thenReturn(cnx);
        Channel channel = mock(Channel.class);
        when(cnx.channel()).thenReturn(channel);
        when(pulsarService.getClient()).thenAnswer(new Answer<PulsarClient>(){

            @Override
            public PulsarClient answer(InvocationOnMock invocation) throws Throwable {
                return mockClient;
            }
        });

        when(channel.isActive()).thenReturn(true);

        @Cleanup("stop")
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();
        TransactionBufferHandlerImpl transactionBufferHandler =
                new TransactionBufferHandlerImpl(pulsarService, hashedWheelTimer, 1000, 3000);
        CompletableFuture<TxnID> endFuture =
                transactionBufferHandler.endTxnOnTopic("test", 1, 1, TxnAction.ABORT, 1);

        Field field = TransactionBufferHandlerImpl.class.getDeclaredField("outstandingRequests");
        field.setAccessible(true);
        ConcurrentSkipListMap<Long, Object> outstandingRequests =
                (ConcurrentSkipListMap<Long, Object>) field.get(transactionBufferHandler);

        assertEquals(outstandingRequests.size(), 1);

        Awaitility.await().atLeast(2, TimeUnit.SECONDS).until(() -> {
            if (outstandingRequests.size() == 0) {
                return true;
            }
            return false;
        });

        try {
            endFuture.get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionBufferClientException.RequestTimeoutException);
        }
    }

    @Test
    public void testTransactionBufferChannelUnActive() throws PulsarServerException {
        PulsarService pulsarService = pulsarServiceList.get(0);
        PulsarClient mockClient = mock(PulsarClientImpl.class);
        CompletableFuture<ClientCnx> completableFuture = new CompletableFuture<>();
        ClientCnx clientCnx = mock(ClientCnx.class);
        completableFuture.complete(clientCnx);
        when(((PulsarClientImpl)mockClient).getConnection(anyString())).thenReturn(completableFuture);
        ChannelHandlerContext cnx = mock(ChannelHandlerContext.class);
        when(clientCnx.ctx()).thenReturn(cnx);
        Channel channel = mock(Channel.class);
        when(cnx.channel()).thenReturn(channel);

        when(channel.isActive()).thenReturn(false);
        when(pulsarService.getClient()).thenAnswer(new Answer<PulsarClient>(){

            @Override
            public PulsarClient answer(InvocationOnMock invocation) throws Throwable {
                return mockClient;
            }
        });

        @Cleanup("stop")
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();
        TransactionBufferHandlerImpl transactionBufferHandler =
                new TransactionBufferHandlerImpl(pulsarServiceList.get(0), hashedWheelTimer, 1000, 3000);
        try {
            transactionBufferHandler.endTxnOnTopic("test", 1, 1, TxnAction.ABORT, 1).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.LookupException);
        }

        when(channel.isActive()).thenReturn(true);

        try {
            transactionBufferHandler.endTxnOnTopic("test", 1, 1, TxnAction.ABORT, 1).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TransactionBufferClientException.RequestTimeoutException);
        }
    }

    @Test
    public void testTransactionBufferLookUp() throws Exception {
        String topic = "persistent://" + namespace + "/testTransactionBufferLookUp";
        String subName = "test";

        String abortTopic = topic + "_abort_sub";
        String commitTopic = topic + "_commit_sub";
        admin.topics().createNonPartitionedTopic(abortTopic);
        admin.topics().createSubscription(abortTopic, subName, MessageId.earliest);

        admin.topics().createNonPartitionedTopic(commitTopic);
        admin.topics().createSubscription(commitTopic, subName, MessageId.earliest);

        tbClient.abortTxnOnSubscription(abortTopic, "test", 1L, 1L, -1L).get();

        tbClient.commitTxnOnSubscription(commitTopic, "test", 1L, 1L, -1L).get();

        tbClient.abortTxnOnTopic(abortTopic, 1L, 1L, -1L).get();
        tbClient.commitTxnOnTopic(commitTopic, 1L, 1L, -1L).get();
    }

    @Test
    public void testTransactionBufferRequestCredits() throws Exception {
        String topic = "persistent://" + namespace + "/testTransactionBufferRequestCredits";
        String subName = "test";

        String abortTopic = topic + "_abort_sub";
        String commitTopic = topic + "_commit_sub";

        admin.topics().createNonPartitionedTopic(abortTopic);
        admin.topics().createSubscription(abortTopic, subName, MessageId.earliest);

        admin.topics().createNonPartitionedTopic(commitTopic);
        admin.topics().createSubscription(commitTopic, subName, MessageId.earliest);

        tbClient.abortTxnOnSubscription(abortTopic, "test", 1L, 1L, -1L).get();
        tbClient.commitTxnOnSubscription(commitTopic, "test", 1L, 1L, -1L).get();

        tbClient.abortTxnOnTopic(abortTopic, 1L, 1L, -1L).get();
        tbClient.commitTxnOnTopic(commitTopic, 1L, 1L, -1L).get();

        assertEquals(tbClient.getAvailableRequestCredits(), 1000);
    }

    @Test
    public void testTransactionBufferPendingRequests() throws Exception {

    }

    @Test
    public void testEndTopicNotExist() throws Exception {
        String topic = "persistent://" + namespace + "/testEndTopicNotExist";
        String sub = "test";

        tbClient.abortTxnOnTopic(topic + "_abort_topic", 1L, 1L, -1L).get();
        tbClient.commitTxnOnTopic(topic + "_commit_topic", 1L, 1L, -1L).get();

        tbClient.abortTxnOnSubscription(topic + "_abort_topic", sub, 1L, 1L, -1L).get();
        tbClient.abortTxnOnSubscription(topic + "_commit_topic", sub, 1L, 1L, -1L).get();
    }

    @Test
    public void testEndSubNotExist() throws Exception {

        String topic = "persistent://" + namespace + "/testEndTopicNotExist";
        String sub = "test";
        admin.topics().createNonPartitionedTopic(topic + "_abort_sub");

        admin.topics().createNonPartitionedTopic(topic + "_commit_sub");

        tbClient.abortTxnOnSubscription(topic + "_abort_topic", sub, 1L, 1L, -1L).get();
        tbClient.abortTxnOnSubscription(topic + "_commit_topic", sub, 1L, 1L, -1L).get();
    }
}
