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

import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerRestEndpointMetricsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setExposePerRestEndpointMetricsInPrometheus(true);
        baseSetup();
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testMetrics() throws Exception {
        admin.tenants().createTenant("test", TenantInfo.builder().allowedClusters(Set.of("test")).build());
        admin.namespaces().createNamespace("test/test");
        String topic = "persistent://test/test/test_" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().getList("test/test");

        // This request will be failed
        try {
            admin.topics().createNonPartitionedTopic("persistent://test1/test1/test1");
        } catch (Exception e) {
            // ignore
        }

        admin.topics().delete(topic, true);
        admin.namespaces().deleteNamespace("test/test");
        admin.tenants().deleteTenant("test");

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, false, output);

        String metricsStr = output.toString(StandardCharsets.UTF_8);
        Multimap<String, PrometheusMetricsClient.Metric> metricsMap = PrometheusMetricsClient.parseMetrics(metricsStr);

        Collection<PrometheusMetricsClient.Metric> latency = metricsMap.get("pulsar_broker_rest_endpoint_latency_ms_sum");
        Collection<PrometheusMetricsClient.Metric> failed = metricsMap.get("pulsar_broker_rest_endpoint_failed_total");

        Assert.assertTrue(latency.size() > 0);
        Assert.assertTrue(failed.size() > 0);

        for (PrometheusMetricsClient.Metric m : latency) {
            Assert.assertNotNull(m.tags.get("cluster"));
            Assert.assertNotNull(m.tags.get("path"));
            Assert.assertNotNull(m.tags.get("method"));
        }

        for (PrometheusMetricsClient.Metric m : failed) {
            Assert.assertNotNull(m.tags.get("cluster"));
            Assert.assertNotNull(m.tags.get("path"));
            Assert.assertNotNull(m.tags.get("method"));
            Assert.assertNotNull(m.tags.get("code"));
        }
    }
}