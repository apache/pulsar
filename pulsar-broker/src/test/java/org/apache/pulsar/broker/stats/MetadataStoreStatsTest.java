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
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = "broker")
public class MetadataStoreStatsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);
        super.baseSetup();
        AuthenticationProviderToken.resetMetrics();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        resetConfig();
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

        for (;;) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            consumer.acknowledge(message);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, false, output);
        String metricsStr = output.toString();
        Multimap<String, PrometheusMetricsTest.Metric> metricsMap = PrometheusMetricsTest.parseMetrics(metricsStr);

        Collection<PrometheusMetricsTest.Metric> opsLatency = metricsMap.get("pulsar_metadata_store_ops_latency_ms" + "_sum");
        Collection<PrometheusMetricsTest.Metric> putBytes = metricsMap.get("pulsar_metadata_store_put_bytes" + "_total");

        Assert.assertTrue(opsLatency.size() > 1);
        Assert.assertTrue(putBytes.size() > 1);

        for (PrometheusMetricsTest.Metric m : opsLatency) {
            Assert.assertEquals(m.tags.get("cluster"), "test");
            String metadataStoreName = m.tags.get("name");
            Assert.assertNotNull(metadataStoreName);
            Assert.assertTrue(metadataStoreName.equals(MetadataStoreConfig.METADATA_STORE)
                    || metadataStoreName.equals(MetadataStoreConfig.CONFIGURATION_METADATA_STORE)
                    || metadataStoreName.equals(MetadataStoreConfig.STATE_METADATA_STORE));
            Assert.assertNotNull(m.tags.get("status"));

            if (m.tags.get("status").equals("success")) {
                if (m.tags.get("type").equals("get")) {
                    Assert.assertTrue(m.value >= 0);
                } else if (m.tags.get("type").equals("del")) {
                    Assert.assertTrue(m.value >= 0);
                } else if (m.tags.get("type").equals("put")) {
                    Assert.assertTrue(m.value >= 0);
                } else {
                    Assert.fail();
                }
            } else {
                if (m.tags.get("type").equals("get")) {
                    Assert.assertTrue(m.value >= 0);
                } else if (m.tags.get("type").equals("del")) {
                    Assert.assertTrue(m.value >= 0);
                } else if (m.tags.get("type").equals("put")) {
                    Assert.assertTrue(m.value >= 0);
                } else {
                    Assert.fail();
                }
            }
        }
        for (PrometheusMetricsTest.Metric m : putBytes) {
            Assert.assertEquals(m.tags.get("cluster"), "test");
            String metadataStoreName = m.tags.get("name");
            Assert.assertNotNull(metadataStoreName);
            Assert.assertTrue(metadataStoreName.equals(MetadataStoreConfig.METADATA_STORE)
                    || metadataStoreName.equals(MetadataStoreConfig.CONFIGURATION_METADATA_STORE)
                    || metadataStoreName.equals(MetadataStoreConfig.STATE_METADATA_STORE));
            Assert.assertTrue(m.value > 0);
        }
    }

}
