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
package org.apache.pulsar.broker.stats.prometheus;

import static org.testng.Assert.*;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.UUID;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PrometheusMetricsGeneratorUtilsTest {

    private static final String LABEL_NAME_CLUSTER = "cluster";

    @Test
    public void testGenerateSystemMetricsWithSpecifyCluster() throws Exception {
        String defaultClusterValue = "cluster_test";
        String specifyClusterValue = "lb_x";
        String metricsName = "label_contains_cluster" + randomString();
        Counter counter = new Counter.Builder()
                .name(metricsName)
                .labelNames(LABEL_NAME_CLUSTER)
                .help("x")
                .register(CollectorRegistry.defaultRegistry);
        counter.labels(specifyClusterValue).inc();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusMetricsGeneratorUtils.generate(defaultClusterValue, out,  Collections.emptyList());
        System.out.println(("metrics 1 :" + out.toString()));
        assertTrue(out.toString().contains(
                String.format("%s{cluster=\"%s\"} 1.0", metricsName, specifyClusterValue)
        ));
        // cleanup
        out.close();
        CollectorRegistry.defaultRegistry.unregister(counter);
    }

    @Test
    public void testGenerateSystemMetricsWithDefaultCluster() throws Exception {
        String defaultClusterValue = "cluster_test";
        String labelName = "lb_name";
        String labelValue = "lb_value";
        // default cluster.
        String metricsName = "label_use_default_cluster" + randomString();
        Counter counter = new Counter.Builder()
                .name(metricsName)
                .labelNames(labelName)
                .help("x")
                .register(CollectorRegistry.defaultRegistry);
        counter.labels(labelValue).inc();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusMetricsGeneratorUtils.generate(defaultClusterValue, out,  Collections.emptyList());
        System.out.println(("metrics 1 :" + out.toString()));
        assertTrue(out.toString().contains(
                String.format("%s{cluster=\"%s\",%s=\"%s\"} 1.0",
                        metricsName, defaultClusterValue, labelName, labelValue)
        ));
        // cleanup
        out.close();
        CollectorRegistry.defaultRegistry.unregister(counter);
    }

    @Test
    public void testGenerateSystemMetricsWithoutCustomizedLabel() throws Exception {
        String defaultClusterValue = "cluster_test";
        // default cluster.
        String metricsName = "label_without_customized_label" + randomString();
        Counter counter = new Counter.Builder()
                .name(metricsName)
                .help("x")
                .register(CollectorRegistry.defaultRegistry);
        counter.inc();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusMetricsGeneratorUtils.generate(defaultClusterValue, out,  Collections.emptyList());
        System.out.println(("metrics 1 :" + out.toString()));
        assertTrue(out.toString().contains(
                String.format("%s{cluster=\"%s\"} 1.0", metricsName, defaultClusterValue)
        ));
        // cleanup
        out.close();
        CollectorRegistry.defaultRegistry.unregister(counter);
    }

    private static String randomString(){
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}
