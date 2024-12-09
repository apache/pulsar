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

import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.apache.pulsar.transaction.coordinator.impl.DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS;
import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Sets;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.common.Attributes;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerStats;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriterConfig;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ManagedLedgerMetricsTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        // wait for shutdown of the broker, this prevents flakiness which could be caused by metrics being
        // unregistered asynchronously. This impacts the execution of the next test method if this would be happening.
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    @Test
    public void testManagedLedgerMetrics() throws Exception {
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(pulsar);

        final String addEntryRateKey = "brk_ml_AddEntryMessagesRate";
        List<Metrics> list1 = metrics.generate();
        Assert.assertTrue(list1.isEmpty());

        var topicName = "persistent://my-property/use/my-ns/my-topic1";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        @Cleanup
        var consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("sub1").subscribe();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        var managedLedgerFactory = (ManagedLedgerFactoryImpl) pulsar.getDefaultManagedLedgerFactory();
        for (Entry<String, ManagedLedger> ledger : managedLedgerFactory.getManagedLedgers().entrySet()) {
            ManagedLedgerMBeanImpl stats = (ManagedLedgerMBeanImpl) ledger.getValue().getStats();
            stats.refreshStats(1, TimeUnit.SECONDS);
        }

        List<Metrics> list2 = metrics.generate();
        Assert.assertEquals(list2.get(0).getMetrics().get(addEntryRateKey), 10.0D);

        for (int i = 0; i < 5; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (Entry<String, ManagedLedger> ledger : managedLedgerFactory.getManagedLedgers().entrySet()) {
            ManagedLedgerMBeanImpl stats = (ManagedLedgerMBeanImpl) ledger.getValue().getStats();
            stats.refreshStats(1, TimeUnit.SECONDS);
        }
        List<Metrics> list3 = metrics.generate();
        Assert.assertEquals(list3.get(0).getMetrics().get(addEntryRateKey), 5.0D);

        // Validate OpenTelemetry metrics.
        var ledgers = managedLedgerFactory.getManagedLedgers();
        var topicNameObj = TopicName.get(topicName);
        var mlName = topicNameObj.getPersistenceNamingEncoding();
        assertThat(ledgers).containsKey(mlName);
        var ml = ledgers.get(mlName);
        var attribCommon = Attributes.of(
                OpenTelemetryAttributes.ML_NAME, mlName,
                OpenTelemetryAttributes.PULSAR_NAMESPACE, topicNameObj.getNamespace()
        );
        var metricReader = pulsarTestContext.getOpenTelemetryMetricReader();

        Awaitility.await().untilAsserted(() -> {
            var otelMetrics = metricReader.collectAllMetrics();
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.BACKLOG_COUNTER, attribCommon, 15);
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.MARK_DELETE_COUNTER, attribCommon, 0);
        });

        for (int i = 0; i < 10; i++) {
            var msg = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }

        Awaitility.await().untilAsserted(() -> {
            var otelMetrics = metricReader.collectAllMetrics();
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.BACKLOG_COUNTER, attribCommon, 5);
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.MARK_DELETE_COUNTER, attribCommon,
                    value -> assertThat(value).isPositive());
        });

        Awaitility.await().untilAsserted(() -> {
            @Cleanup
            var cons = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(BrokerTestUtil.newUniqueName("sub"))
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            cons.receive(1, TimeUnit.SECONDS);

            var attribSucceed = Attributes.of(
                    OpenTelemetryAttributes.ML_NAME, mlName,
                    OpenTelemetryAttributes.PULSAR_NAMESPACE, topicNameObj.getNamespace(),
                    OpenTelemetryAttributes.ML_OPERATION_STATUS, "success"
            );
            var attribFailed = Attributes.of(
                    OpenTelemetryAttributes.ML_NAME, mlName,
                    OpenTelemetryAttributes.PULSAR_NAMESPACE, topicNameObj.getNamespace(),
                    OpenTelemetryAttributes.ML_OPERATION_STATUS, "failure"
            );
            var otelMetrics = metricReader.collectAllMetrics();
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.ADD_ENTRY_COUNTER, attribSucceed, 15);
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.ADD_ENTRY_COUNTER, attribFailed, 0);
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.BYTES_OUT_COUNTER, attribCommon,
                    value -> assertThat(value).isPositive());
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.BYTES_OUT_WITH_REPLICAS_COUNTER,
                    attribCommon, value -> assertThat(value).isPositive());

            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.READ_ENTRY_COUNTER, attribSucceed,
                    value -> assertThat(value).isPositive());
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.READ_ENTRY_COUNTER, attribFailed, 0);
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.BYTES_IN_COUNTER, attribCommon,
                    value -> assertThat(value).isPositive());
            assertMetricLongSumValue(otelMetrics, OpenTelemetryManagedLedgerStats.READ_ENTRY_CACHE_MISS_COUNTER,
                    attribCommon, value -> assertThat(value).isPositive());
        });
    }

    @Test
    public void testTransactionTopic() throws Exception {
        TxnLogBufferedWriterConfig txnLogBufferedWriterConfig = new TxnLogBufferedWriterConfig();
        txnLogBufferedWriterConfig.setBatchEnabled(false);
        HashedWheelTimer transactionTimer = new HashedWheelTimer(new DefaultThreadFactory("transaction-timer"),
                1, TimeUnit.MILLISECONDS);
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        createTransactionCoordinatorAssign();
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setMaxEntriesPerLedger(2);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(TransactionCoordinatorID.get(0),
                pulsar.getDefaultManagedLedgerFactory(), managedLedgerConfig, txnLogBufferedWriterConfig,
                transactionTimer, DISABLED_BUFFERED_WRITER_METRICS);
        mlTransactionLog.initialize().get(2, TimeUnit.SECONDS);
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(pulsar);
        metrics.generate();
        // cleanup.
        mlTransactionLog.closeAsync().get();
        transactionTimer.stop();
    }

}
