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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.semconv.SemanticAttributes;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerRestEndpointMetricsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        baseSetup();
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder builder) {
        super.customizeMainPulsarTestContextBuilder(builder);
        builder.enableOpenTelemetry(true);
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

        Collection<MetricData> metricDatas = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        Optional<MetricData> optional = metricDatas.stream().filter(m -> m.getName()
                .equals("pulsar_broker_rest_endpoint_latency")).findFirst();
        Assert.assertTrue(optional.isPresent());

        MetricData metricData = optional.get();
        Assert.assertFalse(metricData.getDescription().isEmpty());
        Assert.assertEquals(metricData.getUnit(), "ms");
        Assert.assertEquals(metricData.getType(), MetricDataType.HISTOGRAM);

        @SuppressWarnings("unchecked")
        Data<HistogramPointData> data = (Data<HistogramPointData>) metricData.getData();
        data.getPoints().forEach(point -> {
            hasAttributes(point);
            Assert.assertTrue(point.getCount() > 0);
            Assert.assertTrue(point.getSum() > 0);
        });

        Assert.assertTrue(hasPoint(data, "/persistent/:tenant/:namespace/:topic", "DELETE"));
        Assert.assertTrue(hasPoint(data, "/persistent/:tenant/:namespace/:topic", "PUT"));
        Assert.assertTrue(hasPoint(data, "/tenants/:tenant", "PUT"));
        Assert.assertTrue(hasPoint(data, "/tenants/:tenant", "DELETE"));
        Assert.assertTrue(hasPoint(data, "/clusters/:cluster", "PUT"));
        Assert.assertTrue(hasPoint(data, "/namespaces/:tenant/:namespace", "PUT"));
        Assert.assertTrue(hasPoint(data, "/namespaces/:tenant/:namespace", "DELETE"));
    }

    private static boolean hasPoint(Data<HistogramPointData> data, String uri, String method) {
        Collection<HistogramPointData> points = data.getPoints();
        for (HistogramPointData point : points) {
            Attributes attrs = point.getAttributes();

            if (attrs.get(SemanticAttributes.HTTP_REQUEST_METHOD).equals(method)
                    && attrs.get(SemanticAttributes.URL_PATH).equals(uri)) {
                return true;
            }
        }

        return false;
    }

    private static void hasAttributes(HistogramPointData data) {
        Attributes attrs = data.getAttributes();
        Assert.assertNotNull(attrs.get(SemanticAttributes.HTTP_REQUEST_METHOD));
        Assert.assertNotNull(attrs.get(SemanticAttributes.URL_PATH));
        Assert.assertNotNull(attrs.get(SemanticAttributes.HTTP_RESPONSE_STATUS_CODE));
    }
}