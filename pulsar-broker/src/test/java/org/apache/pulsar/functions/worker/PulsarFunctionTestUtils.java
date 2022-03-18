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
package org.apache.pulsar.functions.worker;

import java.nio.charset.StandardCharsets;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class PulsarFunctionTestUtils {
    public static String getPrometheusMetrics(int metricsPort) throws IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL(String.format("http://%s:%s/metrics", "localhost", metricsPort));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line + System.lineSeparator());
        }
        rd.close();
        return result.toString();
    }

    @Test
    void testParseMetrics() throws IOException {
        String sampleMetrics = IOUtils.toString(getClass().getClassLoader()
                        .getResourceAsStream("prometheus_metrics_sample.txt"), StandardCharsets.UTF_8);
        parseMetrics(sampleMetrics);
    }

    /**
     * Hacky parsing of Prometheus text format. Should be good enough for unit tests
     *
     * This parser doesn't handle parsing multiple lines for a single metric key.
     * There another implementation in {@link org.apache.pulsar.broker.stats.PrometheusMetricsTest}
     * that supports parsing multiple lines.
     */
    public static Map<String, Metric> parseMetrics(String metrics) {
        final Map<String, Metric> parsed = new HashMap<>();
        // Example of lines are
        // jvm_threads_current{cluster="standalone",} 203.0
        // or
        // pulsar_subscriptions_count{cluster="standalone", namespace="sample/standalone/ns1",
        // topic="persistent://sample/standalone/ns1/test-2"} 0.0 1517945780897
        Pattern pattern = Pattern.compile("^(\\w+)(\\{[^\\}]+\\})?\\s([+-]?[\\d\\w\\.-]+)(\\s(\\d+))?$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");
        Arrays.asList(metrics.split("\n")).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }
            Matcher matcher = pattern.matcher(line);
            checkArgument(matcher.matches(), "Cannot parse metrics from line: '" + line + "'");
            String name = matcher.group(1);
            Metric m = new Metric();
            String numericValue = matcher.group(3);
            if (numericValue.equalsIgnoreCase("-Inf")) {
                m.value = Double.NEGATIVE_INFINITY;
            } else if (numericValue.equalsIgnoreCase("+Inf")) {
                m.value = Double.POSITIVE_INFINITY;
            } else {
                m.value = Double.parseDouble(numericValue);
            }
            String tags = matcher.group(2);
            if (tags != null) {
                tags = tags.replace("{", "").replace("}", "");
                Matcher tagsMatcher = tagsPattern.matcher(tags);
                while (tagsMatcher.find()) {
                    String tag = tagsMatcher.group(1);
                    String value = tagsMatcher.group(2);
                    m.tags.put(tag, value);
                }
            }
            parsed.put(name, m);
        });

        return parsed;
    }

    @ToString
    public static class Metric {
        public final Map<String, String> tags = new TreeMap<>();
        public double value;
    }
}
