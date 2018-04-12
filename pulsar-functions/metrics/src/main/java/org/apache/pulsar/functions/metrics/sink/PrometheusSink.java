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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.google.common.cache.Cache;

import lombok.Getter;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;

/**
 * A web sink that exposes and endpoint that Prometheus can scrape
 *
 * metrics are generated in a text format and separated with a newline "\n"
 * https://prometheus.io/docs/instrumenting/exposition_formats
 *
 * metrics format:
 * heron_metric{topology="topology-name",component="component-id",instance="instance-id"} value timestamp
 */
public class PrometheusSink extends AbstractWebSink {
    private static final Logger LOG = Logger.getLogger(PrometheusSink.class.getName());

    private static final String PREFIX = "pulsar_function";

    private static final String DELIMITER = "\n";

    // This is the cache that is used to serve the metrics
    @Getter
    private Cache<String, Map<String, Double>> metricsCache;

    public PrometheusSink() {
        super();
    }

    @Override
    void initialize(Map<String, String> configuration) {
        metricsCache = createCache();
    }

    @Override
    byte[] generateResponse() throws IOException {
        metricsCache.cleanUp();
        final Map<String, Map<String, Double>> metrics = metricsCache.asMap();
        final StringBuilder sb = new StringBuilder();

        metrics.forEach((String source, Map<String, Double> sourceMetrics) -> {
            String tenant = FunctionDetailsUtils.extractTenantFromFQN(source);
            String namespace = FunctionDetailsUtils.extractNamespaceFromFQN(source);
            String name = FunctionDetailsUtils.extractFunctionNameFromFQN(source);

            sourceMetrics.forEach((String metricName, Double value) -> {

                String exportedMetricName = String.format("%s_%s", PREFIX, metricName);
                sb.append(Prometheus.sanitizeMetricName(exportedMetricName))
                        .append("{")
                        .append("tenant=\"").append(tenant).append("\",")
                        .append("namespace=\"").append(namespace).append("\",")
                        .append("functionname=\"").append(name).append("\"");

                sb.append("} ")
                        .append(Prometheus.doubleToGoString(value))
                        .append(" ").append(currentTimeMillis())
                        .append(DELIMITER);
            });
        });

        return sb.toString().getBytes();
    }

    @Override
    public void processRecord(MetricsData record, Function.FunctionDetails functionDetails) {
        final String source = FunctionDetailsUtils.getFullyQualifiedName(functionDetails);

        Map<String, Double> sourceCache = metricsCache.getIfPresent(source);
        if (sourceCache == null) {
            final Cache<String, Double> newSourceCache = createCache();
            sourceCache = newSourceCache.asMap();
        }

        sourceCache.putAll(processMetrics(record));
        metricsCache.put(source, sourceCache);
    }

    static Map<String, Double> processMetrics(MetricsData metrics) {
        Map<String, Double> map = new HashMap<>();
        for (Map.Entry<String, MetricsData.DataDigest> entry : metrics.getMetricsMap().entrySet()) {
            map.put(entry.getKey() + "_count", entry.getValue().getCount());
            map.put(entry.getKey() + "_sum", entry.getValue().getSum());
            map.put(entry.getKey() + "_max", entry.getValue().getMax());
            map.put(entry.getKey() + "_min", entry.getValue().getMin());
        }

        return map;
    }

    long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    // code taken from prometheus java_client repo
    static final class Prometheus {
        private static final Pattern METRIC_NAME_RE = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
        private static final Pattern METRIC_LABEL_NAME_RE = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
        private static final Pattern RESERVED_METRIC_LABEL_NAME_RE = Pattern.compile("__.*");

        /**
         * Throw an exception if the metric name is invalid.
         */
        static void checkMetricName(String name) {
            if (!METRIC_NAME_RE.matcher(name).matches()) {
                throw new IllegalArgumentException("Invalid metric name: " + name);
            }
        }

        private static final Pattern SANITIZE_PREFIX_PATTERN = Pattern.compile("^[^a-zA-Z_]");
        private static final Pattern SANITIZE_BODY_PATTERN = Pattern.compile("[^a-zA-Z0-9_]");

        /**
         * Sanitize metric name
         */
        static String sanitizeMetricName(String metricName) {
            return SANITIZE_BODY_PATTERN.matcher(
                    SANITIZE_PREFIX_PATTERN.matcher(metricName).replaceFirst("_")
            ).replaceAll("_");
        }

        /**
         * Throw an exception if the metric label name is invalid.
         */
        static void checkMetricLabelName(String name) {
            if (!METRIC_LABEL_NAME_RE.matcher(name).matches()) {
                throw new IllegalArgumentException("Invalid metric label name: " + name);
            }
            if (RESERVED_METRIC_LABEL_NAME_RE.matcher(name).matches()) {
                throw new IllegalArgumentException(
                        "Invalid metric label name, reserved for internal use: " + name);
            }
        }

        /**
         * Convert a double to its string representation in Go.
         */
        static String doubleToGoString(double d) {
            if (d == Double.POSITIVE_INFINITY) {
                return "+Inf";
            }
            if (d == Double.NEGATIVE_INFINITY) {
                return "-Inf";
            }
            if (Double.isNaN(d)) {
                return "NaN";
            }
            return Double.toString(d);
        }

        private Prometheus() {
        }
    }
}