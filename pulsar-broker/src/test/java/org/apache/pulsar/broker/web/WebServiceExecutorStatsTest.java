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
package org.apache.pulsar.broker.web;

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.testng.annotations.Test;

/**
 * {@link WebExecutorStatsTest} calls {@link WebExecutorStats#getStats} itself. This test starts a {@link WebService}
 * instead, so that the gauges only exist when the web service registers them.
 */
@Test(groups = "broker")
public class WebServiceExecutorStatsTest extends SharedPulsarBaseTest {

    @Test
    public void testStartedWebServiceExposesTheExecutorGauges() throws Exception {
        ServiceConfiguration config = getConfig();
        Optional<Integer> brokerWebServicePort = config.getWebServicePort();
        WebService webService = null;
        // The gauges belong to a JVM-wide singleton that the first closed web service unregisters for good. Forget
        // it, so that the WebService started here has to register them itself.
        WebExecutorStats.resetForTesting();
        try {
            // a second web service of the shared broker, on its own port
            config.setWebServicePort(Optional.of(0));
            try {
                webService = new WebService(getPulsar());
                webService.start();
            } finally {
                // the web service sets the port that it listens on in the configuration
                config.setWebServicePort(brokerWebServicePort);
            }

            ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
            PrometheusMetricsTestUtil.generate(getPulsar(), false, false, false, statsOut);
            Multimap<String, Metric> metrics = parseMetrics(statsOut.toString());

            for (String family : List.of("pulsar_web_executor_max_threads", "pulsar_web_executor_min_threads",
                    "pulsar_web_executor_active_threads", "pulsar_web_executor_idle_threads",
                    "pulsar_web_executor_current_threads")) {
                assertThat(metrics.get(family)).as(family).hasSize(1)
                        .allSatisfy(metric -> assertThat(metric.tags).containsKey("cluster"));
            }
        } finally {
            try {
                if (webService != null) {
                    webService.close();
                }
            } finally {
                // Do not leave the closed singleton behind: a web service started later registers the gauges again.
                WebExecutorStats.resetForTesting();
            }
        }
    }
}
