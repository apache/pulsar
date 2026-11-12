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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * Temporary simpleclient exposition, removed after its last collector is migrated.
 * Converting to 1.x snapshots would change legacy TYPE names, created samples and numeric formatting.
 */
final class LegacyPrometheusMetricsWriter {
    private LegacyPrometheusMetricsWriter() {
    }

    /** What the legacy registry wrote: its family names, and the names on its {@code # TYPE} lines. */
    record DeclaredNames(Set<String> families, Set<String> typeNames) {
    }

    /** Writes the registry and returns the names it declared, so the caller can spot duplicates. */
    static DeclaredNames write(SimpleTextOutputStream stream, String cluster, CollectorRegistry registry) {
        DeclaredNames names = new DeclaredNames(new HashSet<>(), new HashSet<>());
        Enumeration<Collector.MetricFamilySamples> families = registry.metricFamilySamples();
        while (families.hasMoreElements()) {
            Collector.MetricFamilySamples family = families.nextElement();
            names.families().add(family.name);
            String typeName = family instanceof PrometheusCounterMetricFamily counter
                    ? counter.getPrometheusTypeName()
                    : family.name + (family.type == Collector.Type.INFO ? "_info" : "");
            names.typeNames().add(typeName);
            stream.write("# TYPE ").write(typeName).write(' ')
                    .write(PrometheusMetricsGeneratorUtils.getTypeStr(family.type)).write('\n');
            for (Collector.MetricFamilySamples.Sample sample : family.samples) {
                stream.write(sample.name).write('{');
                if (!sample.labelNames.contains("cluster")) {
                    writeLabel(stream, "cluster", cluster);
                    if (!sample.labelNames.isEmpty()) {
                        stream.write(',');
                    }
                }
                for (int i = 0; i < sample.labelNames.size(); i++) {
                    if (i > 0) {
                        stream.write(',');
                    }
                    writeLabel(stream, sample.labelNames.get(i), sample.labelValues.get(i));
                }
                stream.write("} ").write(Collector.doubleToGoString(sample.value)).write('\n');
            }
        }
        return names;
    }

    private static void writeLabel(SimpleTextOutputStream stream, String name, String value) {
        stream.write(name).write("=\"").write(PulsarPrometheusTextOutput.escapeLabelValue(value)).write('"');
    }
}
