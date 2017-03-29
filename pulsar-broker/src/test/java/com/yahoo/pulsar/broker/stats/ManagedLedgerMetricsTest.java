/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.stats;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.broker.service.BrokerTestBase;
import com.yahoo.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import com.yahoo.pulsar.client.api.Producer;

import junit.framework.Assert;

/**
 */
public class ManagedLedgerMetricsTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testManagedLedgerMetrics() throws Exception {
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(pulsar);

        final String addEntryRateKey = "brk_ml_AddEntryMessagesRate";
        List<Metrics> list1 = metrics.generate();
        Assert.assertTrue(list1.isEmpty());

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1");
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        for (Entry<String, ManagedLedgerImpl> ledger : ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory())
                .getManagedLedgers().entrySet()) {
            ManagedLedgerMBeanImpl stats = (ManagedLedgerMBeanImpl) ledger.getValue().getStats();
            stats.refreshStats(1, TimeUnit.SECONDS);
        }

        List<Metrics> list2 = metrics.generate();
        Assert.assertEquals(list2.get(0).getMetrics().get(addEntryRateKey), 10.0D);

        for (int i = 0; i < 5; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (Entry<String, ManagedLedgerImpl> ledger : ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory())
                .getManagedLedgers().entrySet()) {
            ManagedLedgerMBeanImpl stats = (ManagedLedgerMBeanImpl) ledger.getValue().getStats();
            stats.refreshStats(1, TimeUnit.SECONDS);
        }
        List<Metrics> list3 = metrics.generate();
        Assert.assertEquals(list3.get(0).getMetrics().get(addEntryRateKey), 5.0D);

    }

    
    @Test
    public void testZkOpStatsMetrics() throws Exception {

        pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1");
        Metrics zkOpMetric = getMetric("zk_op_stats");
        Assert.assertNotNull(zkOpMetric);
        Assert.assertTrue(zkOpMetric.getMetrics().containsKey("zk_latency_write_99_99_percentile_ms"));
        Assert.assertTrue(zkOpMetric.getMetrics().containsKey("zk_latency_read_99_99_percentile_ms"));
        Assert.assertTrue((long) zkOpMetric.getMetrics().get("zk_read_rate") > 0);
        Assert.assertTrue((long) zkOpMetric.getMetrics().get("zk_write_rate") > 0);

        // create another topic
        pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic2");
        zkOpMetric = getMetric("zk_op_stats");
        // save read and write rate per topic: which should be the same for all topics
        long readRate = (long) zkOpMetric.getMetrics().get("zk_read_rate");
        long writeRete = (long) zkOpMetric.getMetrics().get("zk_write_rate");
        Assert.assertTrue(readRate > 0);
        Assert.assertTrue(writeRete > 0);

        // create three new topics : which should create thrice read/write rate compare to previous one
        pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic3");
        pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic4");
        pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic5");
        zkOpMetric = getMetric("zk_op_stats");
        long readRate2 = (long) zkOpMetric.getMetrics().get("zk_read_rate");
        long writeRete2 = (long) zkOpMetric.getMetrics().get("zk_write_rate");
        Assert.assertEquals(readRate2, 3 * readRate);
        Assert.assertEquals(writeRete2, 3 * writeRete);

        // same topic doesn't create any zk-operation
        pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic5");
        zkOpMetric = getMetric("zk_op_stats");
        readRate2 = (long) zkOpMetric.getMetrics().get("zk_read_rate");
        writeRete2 = (long) zkOpMetric.getMetrics().get("zk_write_rate");
        Assert.assertEquals(readRate2, 0);
        Assert.assertEquals(writeRete2, 0);

    }

    private Metrics getMetric(String dimension) {
        BrokerService brokerService = pulsar.getBrokerService();
        brokerService.updateRates();
        for (Metrics metric : brokerService.getDestinationMetrics()) {
            if (dimension.equalsIgnoreCase(metric.getDimension("metric"))) {
                return metric;
            }
        }
        return null;
    }
    
}
