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
import java.io.IOException;
import java.io.Writer;
import java.util.Enumeration;
import org.apache.bookkeeper.stats.Counter;

/**
 * Logic to write metrics in Prometheus text format.
 */
public class PrometheusTextFormatUtil {
    static void writeGauge(Writer w, String name, String cluster, SimpleGauge<? extends Number> gauge) {
        // Example:
        // # TYPE bookie_client_bookkeeper_ml_scheduler_completed_tasks_0 gauge
        // pulsar_bookie_client_bookkeeper_ml_scheduler_completed_tasks_0{cluster="pulsar"} 1044057
        try {
            w.append("# TYPE ").append(name).append(" gauge\n");
            w.append(name).append("{cluster=\"").append(cluster).append("\"}")
                    .append(' ').append(gauge.getSample().toString()).append('\n');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void writeCounter(Writer w, String name, String cluster, Counter counter) {
        // Example:
        // # TYPE jvm_threads_started_total counter
        // jvm_threads_started_total{cluster="test"} 59
        try {
            w.append("# TYPE ").append(name).append(" counter\n");
            w.append(name).append("{cluster=\"").append(cluster).append("\"}")
                    .append(' ').append(counter.get().toString()).append('\n');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void writeOpStat(Writer w, String name, String cluster, DataSketchesOpStatsLogger opStat) {
        // Example:
        // # TYPE pulsar_bookie_client_bookkeeper_ml_workers_task_queued summary
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="false",
        // quantile="0.5"} NaN
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="false",
        // quantile="0.75"} NaN
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="false",
        // quantile="0.95"} NaN
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="false",
        // quantile="0.99"} NaN
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="false",
        // quantile="0.999"} NaN
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="false",
        // quantile="0.9999"} NaN
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="false",
        // quantile="1.0"} -Infinity
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued_count{cluster="pulsar", success="false"} 0
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued_sum{cluster="pulsar", success="false"} 0.0
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="true",
        // quantile="0.5"} 0.031
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="true",
        // quantile="0.75"} 0.043
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="true",
        // quantile="0.95"} 0.061
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="true",
        // quantile="0.99"} 0.064
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="true",
        // quantile="0.999"} 0.073
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="true",
        // quantile="0.9999"} 0.073
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued{cluster="pulsar", success="true",
        // quantile="1.0"} 0.552
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued_count{cluster="pulsar", success="true"} 40911432
        // pulsar_bookie_client_bookkeeper_ml_workers_task_queued_sum{cluster="pulsar", success="true"} 527.0
        try {
            w.append("# TYPE ").append(name).append(" summary\n");
            writeQuantile(w, opStat, name, cluster, false, 0.5);
            writeQuantile(w, opStat, name, cluster, false, 0.75);
            writeQuantile(w, opStat, name, cluster, false, 0.95);
            writeQuantile(w, opStat, name, cluster, false, 0.99);
            writeQuantile(w, opStat, name, cluster, false, 0.999);
            writeQuantile(w, opStat, name, cluster, false, 0.9999);
            writeQuantile(w, opStat, name, cluster, false, 1.0);
            writeCount(w, opStat, name, cluster, false);
            writeSum(w, opStat, name, cluster, false);

            writeQuantile(w, opStat, name, cluster, true, 0.5);
            writeQuantile(w, opStat, name, cluster, true, 0.75);
            writeQuantile(w, opStat, name, cluster, true, 0.95);
            writeQuantile(w, opStat, name, cluster, true, 0.99);
            writeQuantile(w, opStat, name, cluster, true, 0.999);
            writeQuantile(w, opStat, name, cluster, true, 0.9999);
            writeQuantile(w, opStat, name, cluster, true, 1.0);
            writeCount(w, opStat, name, cluster, true);
            writeSum(w, opStat, name, cluster, true);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeQuantile(Writer w, DataSketchesOpStatsLogger opStat, String name, String cluster,
                                      Boolean success, double quantile) throws IOException {
        w.append(name).append("{cluster=\"").append(cluster).append("\", success=\"")
                .append(success.toString()).append("\", quantile=\"")
                .append(Double.toString(quantile)).append("\"} ")
                .append(Double.toString(opStat.getQuantileValue(success, quantile))).append('\n');
    }

    private static void writeCount(Writer w, DataSketchesOpStatsLogger opStat, String name, String cluster,
                                   Boolean success) throws IOException {
        w.append(name).append("_count{cluster=\"").append(cluster).append("\", success=\"")
                .append(success.toString()).append("\"} ")
                .append(Long.toString(opStat.getCount(success))).append('\n');
    }

    private static void writeSum(Writer w, DataSketchesOpStatsLogger opStat, String name, String cluster,
                                 Boolean success) throws IOException {
        w.append(name).append("_sum{cluster=\"").append(cluster).append("\", success=\"")
                .append(success.toString()).append("\"} ")
                .append(Double.toString(opStat.getSum(success))).append('\n');
    }

    public static void writeMetricsCollectedByPrometheusClient(Writer w, CollectorRegistry registry)
            throws IOException {
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
}
