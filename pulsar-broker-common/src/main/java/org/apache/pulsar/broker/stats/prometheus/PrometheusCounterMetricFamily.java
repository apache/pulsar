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
package org.apache.pulsar.broker.stats.prometheus;

import io.prometheus.client.CounterMetricFamily;
import java.util.List;

/**
 * Opt-in counter family whose Prometheus text TYPE declaration uses its {@code _total} sample name.
 *
 * <p>The Prometheus client normalizes counter family names by removing {@code _total}. New custom
 * collectors can use this family to request the matching TYPE name from
 * {@link PrometheusMetricsGeneratorUtils}, without changing how existing collectors are exported.
 * Only total samples are added by this family; it does not generate {@code _created} samples.
 */
public final class PrometheusCounterMetricFamily extends CounterMetricFamily {
    public PrometheusCounterMetricFamily(String name, String help, List<String> labelNames) {
        super(name, help, labelNames);
    }

    String getPrometheusTypeName() {
        return name + "_total";
    }
}
