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
package org.apache.pulsar.broker.stats;

import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStoreProvider;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStoreProvider;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionBatchWriterMetricsTest extends MockedPulsarServiceBaseTest {

    private final String clusterName = "test";
    public static final NamespaceName DEFAULT_NAMESPACE = NamespaceName.get("public/default");
    private final String topicNameSuffix = "t-rest-topic";
    private final String topicName = DEFAULT_NAMESPACE.getPersistentTopicName(topicNameSuffix);

    @BeforeClass
    public void setup() throws Exception {
        MLTransactionMetadataStoreProvider.initBufferedWriterMetrics("localhost");
        MLPendingAckStoreProvider.initBufferedWriterMetrics("localhost");
        super.internalSetup();
    }

    @AfterClass
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // enable transaction.
        conf.setSystemTopicEnabled(true);
        conf.setTransactionCoordinatorEnabled(true);
        // enabled batch writer.
        conf.setTransactionPendingAckBatchedWriteEnabled(true);
        conf.setTransactionPendingAckBatchedWriteMaxRecords(10);
        conf.setTransactionLogBatchedWriteEnabled(true);
        conf.setTransactionLogBatchedWriteMaxRecords(10);
    }

    @Override
    protected PulsarService startBroker(ServiceConfiguration conf) throws Exception {
        PulsarService pulsar = startBrokerWithoutAuthorization(conf);
        ensureClusterExists(pulsar, clusterName);
        ensureTenantExists(pulsar.getPulsarResources().getTenantResources(), TopicName.PUBLIC_TENANT, clusterName);
        ensureNamespaceExists(pulsar.getPulsarResources().getNamespaceResources(), DEFAULT_NAMESPACE,
                clusterName);
        ensureNamespaceExists(pulsar.getPulsarResources().getNamespaceResources(), NamespaceName.SYSTEM_NAMESPACE,
                clusterName);
        ensureTopicExists(pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources(),
                SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN, 16);
        return pulsar;
    }

    @Test
    public void testTransactionMetaLogMetrics() throws Exception{
        String metricsLabelCluster = clusterName;
        String metricsLabelBroker = pulsar.getAdvertisedAddress().split(":")[0];
        admin.topics().createNonPartitionedTopic(topicName);

        sendAndAckSomeMessages();

        // call metrics
        @Cleanup
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(brokerUrl + "/metrics/get");
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).buildGet().invoke();
        Assert.assertTrue(response.getStatus() / 200 == 1);
        List<String> metricsLines = parseResponseEntityToList(response);

        metricsLines = metricsLines.stream().filter(s -> !s.startsWith("#") && s.contains("bufferedwriter")).collect(
                Collectors.toList());

        // verify tc.
        String metrics_key_txn_tc_record_count_sum =
                "pulsar_txn_tc_bufferedwriter_batch_records_sum{cluster=\"%s\",broker=\"%s\"} ";
        Assert.assertTrue(searchMetricsValue(metricsLines,
                String.format(metrics_key_txn_tc_record_count_sum, metricsLabelCluster, metricsLabelBroker))
                > 0);
        String metrics_key_txn_tc_max_delay =
                "pulsar_txn_tc_bufferedwriter_flush_trigger_max_delay_total{cluster=\"%s\",broker=\"%s\"} ";
        Assert.assertTrue(searchMetricsValue(metricsLines,
                String.format(metrics_key_txn_tc_max_delay, metricsLabelCluster, metricsLabelBroker))
                > 0);
        String metrics_key_txn_tc_bytes_size =
                "pulsar_txn_tc_bufferedwriter_batch_size_bytes_sum{cluster=\"%s\",broker=\"%s\"} ";
        Assert.assertTrue(searchMetricsValue(metricsLines,
                String.format(metrics_key_txn_tc_bytes_size, metricsLabelCluster, metricsLabelBroker))
                > 0);
        // verify pending ack.
        String metrics_key_txn_pending_ack_record_count_sum =
                "pulsar_txn_pending_ack_store_bufferedwriter_batch_records_sum{cluster=\"%s\",broker=\"%s\"} ";
        Assert.assertTrue(searchMetricsValue(metricsLines,
                String.format(metrics_key_txn_pending_ack_record_count_sum, metricsLabelCluster, metricsLabelBroker))
                > 0);
        String metrics_key_txn_pending_ack_max_delay =
                "pulsar_txn_pending_ack_store_bufferedwriter_flush_trigger_max_delay_total{cluster=\"%s\",broker=\"%s\"} ";
        Assert.assertTrue(searchMetricsValue(metricsLines,
                String.format(metrics_key_txn_pending_ack_max_delay, metricsLabelCluster, metricsLabelBroker))
                > 0);
        String metrics_key_txn_pending_ack_bytes_size =
                "pulsar_txn_pending_ack_store_bufferedwriter_batch_size_bytes_sum{cluster=\"%s\",broker=\"%s\"} ";
        Assert.assertTrue(searchMetricsValue(metricsLines,
                String.format(metrics_key_txn_pending_ack_bytes_size, metricsLabelCluster, metricsLabelBroker))
                > 0);

        // cleanup.
        response.close();
        admin.topics().delete(topicName, true);
    }

    private static Double searchMetricsValue(List<String> metricsLines, String key){
        for (int i = 0; i < metricsLines.size(); i++){
            String metricsLine = metricsLines.get(i);
            if (metricsLine.startsWith("#")){
                continue;
            }
            if (metricsLine.startsWith(key)){
                return Double.valueOf(metricsLine.split(" ")[1]);
            }
        }
        return null;
    }

    private void sendAndAckSomeMessages() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topicName)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .batchingMaxMessages(2)
                .create();
        Consumer consumer = pulsarClient.newConsumer()
                .subscriptionType(SubscriptionType.Shared)
                .topic(topicName)
                .isAckReceiptEnabled(true)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionName("my-subscription")
                .subscribe();
        producer.sendAsync("normal message x".getBytes()).get();
        for (int i = 0; i < 100; i++){
            Transaction transaction =
                    pulsarClient.newTransaction().withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
            Message msg = consumer.receive();
            producer.newMessage(transaction).value(("tx msg a-" + i).getBytes(StandardCharsets.UTF_8)).sendAsync();
            producer.newMessage(transaction).value(("tx msg b-" + i).getBytes(StandardCharsets.UTF_8)).sendAsync();
            consumer.acknowledgeAsync(msg.getMessageId(), transaction);
            transaction.commit().get();
        }
    }

    private static void ensureClusterExists(PulsarService pulsar, String cluster) throws Exception {
        ClusterResources clusterResources = pulsar.getPulsarResources().getClusterResources();
        ClusterData clusterData = ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .serviceUrlTls(pulsar.getWebServiceAddress())
                .brokerServiceUrl(pulsar.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar.getBrokerServiceUrl())
                .build();
        if (!clusterResources.clusterExists(cluster)) {
            clusterResources.createCluster(cluster, clusterData);
        }
    }

    private static void ensureTopicExists(NamespaceResources.PartitionedTopicResources partitionedTopicResources,
                                          TopicName topicName, int numPartitions) throws Exception {
        Optional<PartitionedTopicMetadata> getResult =
                partitionedTopicResources.getPartitionedTopicMetadataAsync(topicName).get();
        if (!getResult.isPresent()) {
            partitionedTopicResources.createPartitionedTopic(topicName, new PartitionedTopicMetadata(numPartitions));
        } else {
            PartitionedTopicMetadata existsMeta = getResult.get();
            if (existsMeta.partitions < numPartitions) {
                partitionedTopicResources.updatePartitionedTopicAsync(topicName,
                        __ -> new PartitionedTopicMetadata(numPartitions)).get();
            }
        }
    }

    private static void ensureNamespaceExists(NamespaceResources namespaceResources, NamespaceName namespaceName,
                                              String cluster) throws Exception {
        if (!namespaceResources.namespaceExists(namespaceName)) {
            Policies policies = new Policies();
            policies.bundles = getBundles(16);
            policies.replication_clusters = Collections.singleton(cluster);
            namespaceResources.createPolicies(namespaceName, policies);
        } else {
            namespaceResources.setPolicies(namespaceName, policies -> {
                policies.replication_clusters.add(cluster);
                return policies;
            });
        }
    }

    private static void ensureTenantExists(TenantResources tenantResources, String tenant, String cluster)
            throws Exception {
        if (!tenantResources.tenantExists(tenant)) {
            TenantInfoImpl publicTenant = new TenantInfoImpl(Collections.emptySet(), Collections.singleton(cluster));
            tenantResources.createTenant(tenant, publicTenant);
        } else {
            tenantResources.updateTenantAsync(tenant, ti -> {
                ti.getAllowedClusters().add(cluster);
                return ti;
            }).get();
        }
    }

    private List<String> parseResponseEntityToList(Response response) throws Exception {
        InputStream inputStream = (InputStream) response.getEntity();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        List<String> list = new ArrayList<>();
        while (true){
            String str = bufferedReader.readLine();
            if (str == null){
                break;
            }
            list.add(str);
        }
        return list;
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        org.apache.pulsar.client.api.ClientBuilder clientBuilder =
                PulsarClient.builder()
                        .serviceUrl(url)
                        .enableTransaction(true)
                        .statsInterval(intervalInSecs, TimeUnit.SECONDS);
        customizeNewPulsarClientBuilder(clientBuilder);
        return createNewPulsarClient(clientBuilder);
    }

}
