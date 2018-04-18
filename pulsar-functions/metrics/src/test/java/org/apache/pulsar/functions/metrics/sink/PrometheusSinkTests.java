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
package org.apache.pulsar.functions.metrics.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class PrometheusSinkTests {

    private static final long NOW = System.currentTimeMillis();

    private final class PrometheusTestSink extends PrometheusSink {

        private PrometheusTestSink() {
        }

        @Override
        protected void startHttpServer(String path, int port) {
            // no need to start the server for tests
        }

        public Map<String, Map<String, Double>> getMetrics() {
            return getMetricsCache().asMap();
        }

        long currentTimeMillis() {
            return NOW;
        }
    }

    private Map<String, String> defaultConf;
    private InstanceCommunication.MetricsData records;

    @BeforeMethod
    public void before() {

        defaultConf = new HashMap<>();
        defaultConf.put("port", "9999");
        defaultConf.put("path", "test");
        defaultConf.put("flat-metrics", "true");
        defaultConf.put("include-topology-name", "false");

        InstanceCommunication.MetricsData.Builder bldr = InstanceCommunication.MetricsData.newBuilder();
        InstanceCommunication.MetricsData.DataDigest metric1 =
                InstanceCommunication.MetricsData.DataDigest.newBuilder()
                        .setCount(2).setSum(5).setMax(3).setMin(2).build();
        bldr.putMetrics("metric_1", metric1);
        InstanceCommunication.MetricsData.DataDigest metric2 =
                InstanceCommunication.MetricsData.DataDigest.newBuilder()
                        .setCount(3).setSum(6).setMax(3).setMin(1).build();
        bldr.putMetrics("metric_2", metric2);

        records = bldr.build();
    }

    @Test
    public void testMetricsGrouping() {
        PrometheusTestSink sink = new PrometheusTestSink();
        sink.init(defaultConf);
        Function.FunctionDetails functionDetails = createFunctionDetails("tenant", "namespace", "functionname");
        sink.processRecord(records, functionDetails);

        final Map<String, Map<String, Double>> metrics = sink.getMetrics();
        assertTrue(metrics.containsKey(FunctionDetailsUtils.getFullyQualifiedName(functionDetails)));
    }

    @Test
    public void testResponse() throws IOException {
        PrometheusTestSink sink = new PrometheusTestSink();
        sink.init(defaultConf);
        Function.FunctionDetails functionDetails = createFunctionDetails("tenant", "namespace", "functionname");
        sink.processRecord(records, functionDetails);

        final List<String> expectedLines = Arrays.asList(
                createMetric(functionDetails, "metric_1_count", 2),
                createMetric(functionDetails, "metric_1_sum", 5),
                createMetric(functionDetails, "metric_1_max", 3),
                createMetric(functionDetails, "metric_1_min", 2),
                createMetric(functionDetails, "metric_2_count", 3),
                createMetric(functionDetails, "metric_2_sum", 6),
                createMetric(functionDetails, "metric_2_max", 3),
                createMetric(functionDetails, "metric_2_min", 1)
        );

        final Set<String> generatedLines =
                new HashSet<>(Arrays.asList(new String(sink.generateResponse()).split("\n")));

        assertEquals(expectedLines.size(), generatedLines.size());

        expectedLines.forEach((String line) -> {
            assertTrue(generatedLines.contains(line));
        });
    }

    private String createMetric(Function.FunctionDetails functionDetails,
                                String metric, double value) {
        return String.format("pulsar_function_%s"
                        + "{tenant=\"%s\",namespace=\"%s\",functionname=\"%s\"}"
                        + " %s %d",
                metric, functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName(), value, NOW);
    }

    private Function.FunctionDetails createFunctionDetails(String tenant, String namespace, String name) {
        Function.FunctionDetails.Builder bldr = Function.FunctionDetails.newBuilder();
        bldr.setTenant(tenant);
        bldr.setNamespace(namespace);
        bldr.setName(name);
        return bldr.build();
    }
}
