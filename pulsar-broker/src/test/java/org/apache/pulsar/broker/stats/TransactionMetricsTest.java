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
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
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

public class TransactionMetricsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration serviceConfiguration = getDefaultConf();
        serviceConfiguration.setTransactionCoordinatorEnabled(true);
        super.baseSetup(serviceConfiguration);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTransactionCoordinatorMetrics() throws Exception{
        long timeout = 10000;
        TransactionCoordinatorID transactionCoordinatorIDOne = TransactionCoordinatorID.get(1);
        TransactionCoordinatorID transactionCoordinatorIDTwo = TransactionCoordinatorID.get(2);
        pulsar.getTransactionMetadataStoreService().addTransactionMetadataStore(transactionCoordinatorIDOne);
        pulsar.getTransactionMetadataStoreService().addTransactionMetadataStore(transactionCoordinatorIDTwo);

        Awaitility.await().atMost(2000,  TimeUnit.MILLISECONDS).until(() ->
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
            if ("1".equals(item.tags.get("coordinator_id"))) {
                assertEquals(item.value, 1);
            } else {
                assertEquals(item.value, 2);
            }
        });
    }

    @Test
    public void testTransactionCoordinatorRateMetrics() throws Exception{
        long timeout = 10000;
        int txnCount = 120;
        String ns1 = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns1);
        String topic = "persistent://" + ns1 + "/test_coordinator_metrics";
        String subName = "test_coordinator_metrics";
        TransactionCoordinatorID transactionCoordinatorIDOne = TransactionCoordinatorID.get(1);
        pulsar.getTransactionMetadataStoreService().addTransactionMetadataStore(transactionCoordinatorIDOne);
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, subName, MessageId.earliest);
        Awaitility.await().atMost(2000,  TimeUnit.MILLISECONDS).until(() ->
                pulsar.getTransactionMetadataStoreService().getStores().size() == 1);

        List<TxnID> list = new ArrayList<>();
        for (int i = 0; i < txnCount; i++) {
            TxnID txnID = pulsar.getTransactionMetadataStoreService().getStores()
                    .get(transactionCoordinatorIDOne).newTransaction(timeout).get();
            list.add(txnID);

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

        Awaitility.await().atMost(2000, TimeUnit.SECONDS).until(() -> {
            try {
                TxnMeta txnMeta = pulsar.getTransactionMetadataStoreService()
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
}
