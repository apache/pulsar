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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SummaryTest {

    @DataProvider
    public Object[][] invalidQuantiles() {
        return new Object[][] {
                {"summary_test_quantile_above_one", new double[] {0.5, 1.5}},
                {"summary_test_negative_quantile", new double[] {-0.1, 0.5}},
                {"summary_test_duplicate_quantile", new double[] {0.5, 0.99, 0.5}},
        };
    }

    @Test(dataProvider = "invalidQuantiles")
    public void registerRejectsInvalidQuantiles(String name, double[] quantiles) {
        Summary.Builder builder = Summary.build(name, "-");
        for (double quantile : quantiles) {
            builder.quantile(quantile);
        }

        // A summary that is registered after all is unregistered at once: its scrape would fail, and with it the
        // scrapes of the tests that run later in this JVM
        assertThatThrownBy(() -> PrometheusRegistry.defaultRegistry.unregister(builder.register()))
                .as("registering %s", name)
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(PrometheusRegistry.defaultRegistry.scrape(name::equals))
                .as("summaries registered as %s", name)
                .isEmpty();
    }
}
