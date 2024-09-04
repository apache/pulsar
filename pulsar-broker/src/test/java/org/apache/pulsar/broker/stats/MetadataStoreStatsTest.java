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

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetricsToken;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = "flaky")
public class MetadataStoreStatsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        AuthenticationMetricsToken.reset();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);
        // wait for shutdown of the broker, this prevents flakiness which could be caused by
        // org.apache.pulsar.metadata.impl.stats.BatchMetadataStoreStats.close method which unregisters metrics
        // asynchronously. This impacts the execution of the next test method if this would be happening.
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMetadataStoreStats() throws Exception {
        String ns = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns);

        String topic = "persistent://prop/ns-abc1/metadata-store-" + UUID.randomUUID();
        String subName = "my-sub1";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName(subName).subscribe();

        for (int i = 0; i < 100; i++) {
            producer.newMessage().value(UUID.randomUUID().toString()).send();
        }

        for (int i = 0; i < 100; i++) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            consumer.acknowledge(message);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, false, output);
        String metricsStr = output.toString();
        Multimap<String, Metric> metricsMap = parseMetrics(metricsStr);

        String metricsDebugMessage = "Assertion failed with metrics:\n" + metricsStr + "\n";

        Collection<Metric> opsLatency = metricsMap.get("pulsar_metadata_store_ops_latency_ms" + "_sum");
        Collection<Metric> putBytes = metricsMap.get("pulsar_metadata_store_put_bytes" + "_total");

        Assert.assertTrue(opsLatency.size() > 1, metricsDebugMessage);
        Assert.assertTrue(putBytes.size() > 1, metricsDebugMessage);

        Set<String> expectedMetadataStoreName = new HashSet<>();
        expectedMetadataStoreName.add(MetadataStoreConfig.METADATA_STORE);
        expectedMetadataStoreName.add(MetadataStoreConfig.CONFIGURATION_METADATA_STORE);

        AtomicInteger matchCount = new AtomicInteger(0);
        for (Metric m : opsLatency) {
            Assert.assertEquals(m.tags.get("cluster"), "test", metricsDebugMessage);
            String metadataStoreName = m.tags.get("name");
            if (!isExpectedLabel(metadataStoreName, expectedMetadataStoreName, matchCount)) {
                continue;
            }
            Assert.assertNotNull(m.tags.get("status"), metricsDebugMessage);

            if (m.tags.get("status").equals("success")) {
                if (m.tags.get("type").equals("get")) {
                    Assert.assertTrue(m.value >= 0, metricsDebugMessage);
                } else if (m.tags.get("type").equals("del")) {
                    Assert.assertTrue(m.value >= 0, metricsDebugMessage);
                } else if (m.tags.get("type").equals("put")) {
                    Assert.assertTrue(m.value >= 0, metricsDebugMessage);
                } else {
                    Assert.fail(metricsDebugMessage);
                }
            } else {
                if (m.tags.get("type").equals("get")) {
                    Assert.assertTrue(m.value >= 0, metricsDebugMessage);
                } else if (m.tags.get("type").equals("del")) {
                    Assert.assertTrue(m.value >= 0, metricsDebugMessage);
                } else if (m.tags.get("type").equals("put")) {
                    Assert.assertTrue(m.value >= 0, metricsDebugMessage);
                } else {
                    Assert.fail(metricsDebugMessage);
                }
            }
        }
        // Because the combination quantity between status(success, fail) and type(get, del, put) is 6.
        Assert.assertEquals(matchCount.get(), expectedMetadataStoreName.size() * 6);

        matchCount = new AtomicInteger(0);
        for (Metric m : putBytes) {
            Assert.assertEquals(m.tags.get("cluster"), "test", metricsDebugMessage);
            String metadataStoreName = m.tags.get("name");
            if (!isExpectedLabel(metadataStoreName, expectedMetadataStoreName, matchCount)) {
                continue;
            }
            Assert.assertTrue(m.value >= 0, metricsDebugMessage);
        }
        Assert.assertEquals(matchCount.get(), expectedMetadataStoreName.size());
    }

    @Test
    public void testBatchMetadataStoreMetrics() throws Exception {
        String ns = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns);

        String topic = "persistent://prop/ns-abc1/metadata-store-" + UUID.randomUUID();
        String subName = "my-sub1";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName(subName).subscribe();

        for (int i = 0; i < 100; i++) {
            producer.newMessage().value(UUID.randomUUID().toString()).send();
        }

        for (int i = 0; i < 100; i++) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            consumer.acknowledge(message);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, false, output);
        String metricsStr = output.toString();
        Multimap<String, Metric> metricsMap = parseMetrics(metricsStr);

        Collection<Metric> executorQueueSize = metricsMap.get("pulsar_batch_metadata_store_executor_queue_size");
        Collection<Metric> opsWaiting = metricsMap.get("pulsar_batch_metadata_store_queue_wait_time_ms" + "_sum");
        Collection<Metric> batchExecuteTime = metricsMap.get("pulsar_batch_metadata_store_batch_execute_time_ms" + "_sum");
        Collection<Metric> opsPerBatch = metricsMap.get("pulsar_batch_metadata_store_batch_size" + "_sum");

        String metricsDebugMessage = "Assertion failed with metrics:\n" + metricsStr + "\n";

        Assert.assertTrue(executorQueueSize.size() > 1, metricsDebugMessage);
        Assert.assertTrue(opsWaiting.size() > 1, metricsDebugMessage);
        Assert.assertTrue(batchExecuteTime.size() > 0, metricsDebugMessage);
        Assert.assertTrue(opsPerBatch.size() > 0, metricsDebugMessage);

        Set<String> expectedMetadataStoreName = new HashSet<>();
        expectedMetadataStoreName.add(MetadataStoreConfig.METADATA_STORE);
        expectedMetadataStoreName.add(MetadataStoreConfig.CONFIGURATION_METADATA_STORE);

        AtomicInteger matchCount = new AtomicInteger(0);
        for (Metric m : executorQueueSize) {
            Assert.assertEquals(m.tags.get("cluster"), "test", metricsDebugMessage);
            String metadataStoreName = m.tags.get("name");
            if (isExpectedLabel(metadataStoreName, expectedMetadataStoreName, matchCount)) {
                continue;
            }
            Assert.assertTrue(m.value >= 0, metricsDebugMessage);
        }
        Assert.assertEquals(matchCount.get(), expectedMetadataStoreName.size());

        matchCount = new AtomicInteger(0);
        for (Metric m : opsWaiting) {
            Assert.assertEquals(m.tags.get("cluster"), "test", metricsDebugMessage);
            String metadataStoreName = m.tags.get("name");
            if (isExpectedLabel(metadataStoreName, expectedMetadataStoreName, matchCount)) {
                continue;
            }
            Assert.assertTrue(m.value >= 0, metricsDebugMessage);
        }
        Assert.assertEquals(matchCount.get(), expectedMetadataStoreName.size());

        matchCount = new AtomicInteger(0);
        for (Metric m : batchExecuteTime) {
            Assert.assertEquals(m.tags.get("cluster"), "test", metricsDebugMessage);
            String metadataStoreName = m.tags.get("name");
            if (isExpectedLabel(metadataStoreName, expectedMetadataStoreName, matchCount)) {
                continue;
            }
            Assert.assertTrue(m.value >= 0, metricsDebugMessage);
        }
        Assert.assertEquals(matchCount.get(), expectedMetadataStoreName.size());

        matchCount = new AtomicInteger(0);
        for (Metric m : opsPerBatch) {
            Assert.assertEquals(m.tags.get("cluster"), "test", metricsDebugMessage);
            String metadataStoreName = m.tags.get("name");
            if (isExpectedLabel(metadataStoreName, expectedMetadataStoreName, matchCount)) {
                continue;
            }
            Assert.assertTrue(m.value >= 0, metricsDebugMessage);
        }
        Assert.assertEquals(matchCount.get(), expectedMetadataStoreName.size());
    }

    private boolean isExpectedLabel(String metadataStoreName, Set<String> expectedLabel,
                                    AtomicInteger expectedLabelCount) {
        if (StringUtils.isEmpty(metadataStoreName)
                || !expectedLabel.contains(metadataStoreName)) {
            return false;
        } else {
            expectedLabelCount.incrementAndGet();
            return true;
        }
    }

}
