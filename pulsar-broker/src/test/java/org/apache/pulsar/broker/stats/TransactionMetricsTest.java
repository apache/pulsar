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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.broker.stats.PrometheusMetricsTest.parseMetrics;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TransactionMetricsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration serviceConfiguration = getDefaultConf();
        serviceConfiguration.setTransactionCoordinatorEnabled(true);
        super.baseSetup(serviceConfiguration);
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                TenantInfo.builder()
                        .adminRoles(Sets.newHashSet("appid1"))
                        .allowedClusters(Sets.newHashSet("test"))
                        .build());
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 1);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTransactionCoordinatorMetrics() throws Exception{
        long timeout = 10000;
        admin.lookups().lookupTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
        TransactionCoordinatorID transactionCoordinatorIDOne = TransactionCoordinatorID.get(0);
        TransactionCoordinatorID transactionCoordinatorIDTwo = TransactionCoordinatorID.get(1);
        pulsar.getTransactionMetadataStoreService().handleTcClientConnect(transactionCoordinatorIDOne);
        pulsar.getTransactionMetadataStoreService().handleTcClientConnect(transactionCoordinatorIDTwo);

        Awaitility.await().until(() ->
                pulsar.getTransactionMetadataStoreService().getStores().size() == 2);
        pulsar.getTransactionMetadataStoreService().getStores()
                .get(transactionCoordinatorIDOne).newTransaction(timeout).get();
        pulsar.getTransactionMetadataStoreService().getStores()
                .get(transactionCoordinatorIDTwo).newTransaction(timeout).get();
        pulsar.getTransactionMetadataStoreService().getStores()
                .get(transactionCoordinatorIDTwo).newTransaction(timeout).get();
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, PrometheusMetricsTest.Metric> metrics = parseMetrics(metricsStr);
        Collection<PrometheusMetricsTest.Metric> metric = metrics.get("pulsar_txn_active_count");
        assertEquals(metric.size(), 2);
        metric.forEach(item -> {
            if ("0".equals(item.tags.get("coordinator_id"))) {
                assertEquals(item.value, 1);
            } else {
                assertEquals(item.value, 2);
            }
        });
    }

    @Test
    public void testTransactionCoordinatorRateMetrics() throws Exception{
        int txnCount = 120;
        String ns1 = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns1);
        String topic = "persistent://" + ns1 + "/test_coordinator_metrics";
        String subName = "test_coordinator_metrics";
        TransactionCoordinatorID transactionCoordinatorIDOne = TransactionCoordinatorID.get(0);
        admin.lookups().lookupPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
        pulsar.getTransactionMetadataStoreService().handleTcClientConnect(transactionCoordinatorIDOne);
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, subName, MessageId.earliest);
        Awaitility.await().atMost(2000,  TimeUnit.MILLISECONDS).until(() ->
                pulsar.getTransactionMetadataStoreService().getStores().size() == 1);


        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName(subName).topic(topic).subscribe();

        List<TxnID> list = new ArrayList<>();
        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true).build();
        for (int i = 0; i < txnCount; i++) {
            TransactionImpl transaction =
                    (TransactionImpl) pulsarClient.newTransaction()
                            .withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
            TxnID txnID = new TxnID(transaction.getTxnIdMostBits(), transaction.getTxnIdLeastBits());
            list.add(txnID);
            if (i == 1) {
                pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true).build();
                consumer.acknowledgeAsync(new MessageIdImpl(1000, 1000, -1), transaction).get();
                continue;
            }

            if (i % 2 == 0) {
                pulsar.getTransactionMetadataStoreService().addProducedPartitionToTxn(list.get(i), Collections.singletonList(topic)).get();
            } else {
                pulsar.getTransactionMetadataStoreService().addAckedPartitionToTxn(list.get(i),
                        Collections.singletonList(TransactionSubscription.builder().topic(topic)
                                .subscription(subName).build())).get();
            }
        }

        for (int i = 0; i < txnCount; i++) {
            if (i % 2 == 0) {
                pulsar.getTransactionMetadataStoreService().endTransaction(list.get(i), TxnAction.COMMIT_VALUE, false).get();
            } else {
                pulsar.getTransactionMetadataStoreService().endTransaction(list.get(i), TxnAction.ABORT_VALUE, false).get();
            }
        }

        pulsar.getBrokerService().updateRates();

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, PrometheusMetricsTest.Metric> metrics = parseMetrics(metricsStr);

        Collection<PrometheusMetricsTest.Metric> metric = metrics.get("pulsar_txn_created_count");
        assertEquals(metric.size(), 1);
        metric.forEach(item -> assertEquals(item.value, txnCount));

        metric = metrics.get("pulsar_txn_committed_count");
        assertEquals(metric.size(), 1);
        metric.forEach(item -> assertEquals(item.value, txnCount / 2));

        metric = metrics.get("pulsar_txn_aborted_count");
        assertEquals(metric.size(), 1);
        metric.forEach(item -> assertEquals(item.value, txnCount / 2));

        TxnID txnID = pulsar.getTransactionMetadataStoreService().getStores()
                .get(transactionCoordinatorIDOne).newTransaction(1000).get();

        Awaitility.await().atMost(2000, TimeUnit.MILLISECONDS).until(() -> {
            try {
               pulsar.getTransactionMetadataStoreService()
                        .getStores().get(transactionCoordinatorIDOne).getTxnMeta(txnID).get();
            } catch (Exception e) {
                return true;
            }
            return false;
        });

        statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        metricsStr = statsOut.toString();
        metrics = parseMetrics(metricsStr);

        metric = metrics.get("pulsar_txn_timeout_count");
        assertEquals(metric.size(), 1);
        metric.forEach(item -> assertEquals(item.value, 1));

        metric = metrics.get("pulsar_txn_append_log_count");
        assertEquals(metric.size(), 1);
        metric.forEach(item -> assertEquals(item.value, txnCount * 4 + 3));

        metric = metrics.get("pulsar_txn_execution_latency_le_5000");
        assertEquals(metric.size(), 1);
        metric.forEach(item -> assertEquals(item.value, 1));

    }

    @Test
    public void testManagedLedgerMetrics() throws Exception{
        String ns1 = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns1);
        String topic = "persistent://" + ns1 + "/test_managed_ledger_metrics";
        String subName = "test_managed_ledger_metrics";
        admin.topics().createNonPartitionedTopic(topic);
        admin.lookups().lookupTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
        TransactionCoordinatorID transactionCoordinatorIDOne = TransactionCoordinatorID.get(0);
        pulsar.getTransactionMetadataStoreService().handleTcClientConnect(transactionCoordinatorIDOne).get();
        admin.topics().createSubscription(topic, subName, MessageId.earliest);

        Awaitility.await().atMost(2000,  TimeUnit.MILLISECONDS).until(() ->
                pulsar.getTransactionMetadataStoreService().getStores().size() == 1);

        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true).build();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .receiverQueueSize(10)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        Transaction transaction =
                pulsarClient.newTransaction().withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
        producer.send("hello pulsar".getBytes());
        consumer.acknowledgeAsync(consumer.receive().getMessageId(), transaction).get();
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, PrometheusMetricsTest.Metric> metrics = parseMetrics(metricsStr);

        Collection<PrometheusMetricsTest.Metric> metric = metrics.get("pulsar_storage_size");
        checkManagedLedgerMetrics(subName, 32, metric);
        checkManagedLedgerMetrics(MLTransactionLogImpl.TRANSACTION_SUBSCRIPTION_NAME, 252, metric);

        metric = metrics.get("pulsar_storage_logical_size");
        checkManagedLedgerMetrics(subName, 16, metric);
        checkManagedLedgerMetrics(MLTransactionLogImpl.TRANSACTION_SUBSCRIPTION_NAME, 126, metric);

        metric = metrics.get("pulsar_storage_backlog_size");
        checkManagedLedgerMetrics(subName, 16, metric);
        checkManagedLedgerMetrics(MLTransactionLogImpl.TRANSACTION_SUBSCRIPTION_NAME, 126, metric);

        statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        metricsStr = statsOut.toString();
        metrics = parseMetrics(metricsStr);
        metric = metrics.get("pulsar_storage_size");
        assertEquals(metric.size(), 3);
        metric = metrics.get("pulsar_storage_logical_size");
        assertEquals(metric.size(), 3);
        metric = metrics.get("pulsar_storage_backlog_size");
        assertEquals(metric.size(), 2);
    }

    private void checkManagedLedgerMetrics(String tag, double value, Collection<PrometheusMetricsTest.Metric> metrics) {
        boolean exist = false;
        for (PrometheusMetricsTest.Metric metric1 : metrics) {
            if (metric1.tags.containsValue(tag)) {
                assertEquals(metric1.value, value);
                exist = true;
            }
        }

        assertTrue(exist);
    }
}
