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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import java.util.Enumeration;
import java.util.Map;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * Logic to write metrics in Prometheus text format.
 */
public class PrometheusTextFormatUtil {
    public static void writeGauge(SimpleTextOutputStream w, String name, SimpleGauge<? extends Number> gauge) {
        // Example:
        // # TYPE bookie_storage_entries_count gauge
        // bookie_storage_entries_count 519
        w.write("# TYPE ").write(name).write(" gauge\n");
        w.write(name);
        writeLabels(w, gauge.getLabels());
        w.write(' ').write(gauge.getSample().toString()).write('\n');

    }

    public static void writeCounter(SimpleTextOutputStream w, String name, LongAdderCounter counter) {
        // Example:
        // # TYPE jvm_threads_started_total counter
        // jvm_threads_started_total 59
        w.write("# TYPE ").write(name).write(" counter\n");
        w.write(name);
        writeLabels(w, counter.getLabels());
        w.write(' ').write(counter.get().toString()).write('\n');
    }

    public static void writeOpStat(SimpleTextOutputStream w, String name, DataSketchesOpStatsLogger opStat) {
        // Example:
        // # TYPE bookie_journal_JOURNAL_ADD_ENTRY summary
        // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.5",} NaN
        // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.75",} NaN
        // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.95",} NaN
        // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.99",} NaN
        // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.999",} NaN
        // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.9999",} NaN
        // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="1.0",} NaN
        // bookie_journal_JOURNAL_ADD_ENTRY_count{success="false",} 0.0
        // bookie_journal_JOURNAL_ADD_ENTRY_sum{success="false",} 0.0
        // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.5",} 1.706
        // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.75",} 1.89
        // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.95",} 2.121
        // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.99",} 10.708
        // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.999",} 10.902
        // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.9999",} 10.902
        // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="1.0",} 10.902
        // bookie_journal_JOURNAL_ADD_ENTRY_count{success="true",} 658.0
        // bookie_journal_JOURNAL_ADD_ENTRY_sum{success="true",} 1265.0800000000002
        w.write("# TYPE ").write(name).write(" summary\n");
        writeQuantile(w, opStat, name, false, 0.5);
        writeQuantile(w, opStat, name, false, 0.75);
        writeQuantile(w, opStat, name, false, 0.95);
        writeQuantile(w, opStat, name, false, 0.99);
        writeQuantile(w, opStat, name, false, 0.999);
        writeQuantile(w, opStat, name, false, 0.9999);
        writeQuantile(w, opStat, name, false, 1.0);
        writeCount(w, opStat, name, false);
        writeSum(w, opStat, name, false);

        writeQuantile(w, opStat, name, true, 0.5);
        writeQuantile(w, opStat, name, true, 0.75);
        writeQuantile(w, opStat, name, true, 0.95);
        writeQuantile(w, opStat, name, true, 0.99);
        writeQuantile(w, opStat, name, true, 0.999);
        writeQuantile(w, opStat, name, true, 0.9999);
        writeQuantile(w, opStat, name, true, 1.0);
        writeCount(w, opStat, name, true);
        writeSum(w, opStat, name, true);
    }

    private static void writeQuantile(SimpleTextOutputStream w, DataSketchesOpStatsLogger opStat, String name,
                                      Boolean success, double quantile) {
        w.write(name)
                .write("{success=\"").write(success.toString())
                .write("\",quantile=\"").write(Double.toString(quantile));
        if (!opStat.getLabels().isEmpty()) {
            w.write("\", ");
            writeLabelsNoBraces(w, opStat.getLabels());
        } else {
            w.write("\"");
        }
        w.write("} ")
                .write(Double.toString(opStat.getQuantileValue(success, quantile))).write('\n');
    }

    private static void writeCount(SimpleTextOutputStream w, DataSketchesOpStatsLogger opStat, String name,
                                   Boolean success) {
        w.write(name).write("_count{success=\"").write(success.toString());
        if (!opStat.getLabels().isEmpty()) {
            w.write("\", ");
            writeLabelsNoBraces(w, opStat.getLabels());
        } else {
            w.write("\"");
        }
        w.write("} ")
                .write(Long.toString(opStat.getCount(success))).write('\n');
    }

    private static void writeSum(SimpleTextOutputStream w, DataSketchesOpStatsLogger opStat, String name,
                                 Boolean success) {
        w.write(name).write("_sum{success=\"").write(success.toString());
        if (!opStat.getLabels().isEmpty()) {
            w.write("\", ");
            writeLabelsNoBraces(w, opStat.getLabels());
        } else {
            w.write("\"");
        }
        w.write("} ")
                .write(Double.toString(opStat.getSum(success))).write('\n');
    }

    public static void writeMetricsCollectedByPrometheusClient(SimpleTextOutputStream w, CollectorRegistry registry) {
        Enumeration<MetricFamilySamples> metricFamilySamples = registry.metricFamilySamples();
        while (metricFamilySamples.hasMoreElements()) {
            MetricFamilySamples metricFamily = metricFamilySamples.nextElement();

            for (int i = 0; i < metricFamily.samples.size(); i++) {
                Sample sample = metricFamily.samples.get(i);
                w.write(sample.name);
                w.write('{');
                for (int j = 0; j < sample.labelNames.size(); j++) {
                    if (j != 0) {
                        w.write(", ");
                    }
                    w.write(sample.labelNames.get(j));
                    w.write("=\"");
                    w.write(sample.labelValues.get(j));
                    w.write('"');
                }

                w.write("} ");
                w.write(Collector.doubleToGoString(sample.value));
                w.write('\n');
            }
        }
    }

    private static void writeLabels(SimpleTextOutputStream w, Map<String, String> labels) {
        if (labels.isEmpty()) {
            return;
        }

        w.write('{');
        writeLabelsNoBraces(w, labels);
        w.write('}');
    }

    private static void writeLabelsNoBraces(SimpleTextOutputStream w, Map<String, String> labels) {
        if (labels.isEmpty()) {
            return;
        }

        boolean isFirst = true;
        for (Map.Entry<String, String> e : labels.entrySet()) {
            if (!isFirst) {
                w.write(',');
            }
            isFirst = false;
            w.write(e.getKey())
                    .write("=\"")
                    .write(e.getValue())
                    .write('"');
        }
    }
}
