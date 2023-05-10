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

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * Helper class to ensure that metrics of the same name are grouped together under the same TYPE header when written.
 * Those are the requirements of the
 * <a href="https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md#grouping-and-sorting">Prometheus Exposition Format</a>.
 */
public class PrometheusMetricStreams {
    private final Map<String, SimpleTextOutputStream> metricStreamMap = new HashMap<>();

    /**
     * Write the given metric and sample value to the stream. Will write #TYPE header if metric not seen before.
     * @param metricName name of the metric.
     * @param value value of the sample
     * @param labelsAndValuesArray varargs of label and label value
     */
    void writeSample(String metricName, Number value, String... labelsAndValuesArray) {
        SimpleTextOutputStream stream = initGaugeType(metricName);
        stream.write(metricName).write('{');
        for (int i = 0; i < labelsAndValuesArray.length; i += 2) {
            String labelValue = labelsAndValuesArray[i + 1];
            if (labelValue != null) {
                labelValue = labelValue.replace("\"", "\\\"");
            }
            stream.write(labelsAndValuesArray[i]).write("=\"").write(labelValue).write('\"');
            if (i + 2 != labelsAndValuesArray.length) {
                stream.write(',');
            }
        }
        stream.write("} ").write(value).write('\n');
    }

    /**
     * Flush all the stored metrics to the supplied stream.
     * @param stream the stream to write to.
     */
    void flushAllToStream(SimpleTextOutputStream stream) {
        metricStreamMap.values().forEach(s -> stream.write(s.getBuffer()));
    }

    /**
     * Release all the streams to clean up resources.
     */
    void releaseAll() {
        metricStreamMap.values().forEach(s -> s.getBuffer().release());
        metricStreamMap.clear();
    }

    private SimpleTextOutputStream initGaugeType(String metricName) {
        return metricStreamMap.computeIfAbsent(metricName, s -> {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(PulsarByteBufAllocator.DEFAULT.directBuffer());
            stream.write("# TYPE ").write(metricName).write(" gauge\n");
            return stream;
        });
    }
}
