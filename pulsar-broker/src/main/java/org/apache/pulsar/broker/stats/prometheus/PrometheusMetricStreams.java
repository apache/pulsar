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
        writeSamplePrefix(metricName, labelsAndValuesArray).write("} ").write(value).write('\n');
    }

    void writeSample(String metricName, long value, String[] labelsAndValuesArray) {
        writeSamplePrefix(metricName, labelsAndValuesArray).write("} ").write(value).write('\n');
    }

    void writeSample(String metricName, double value, String[] labelsAndValuesArray) {
        writeSamplePrefix(metricName, labelsAndValuesArray).write("} ").write(value).write('\n');
    }

    void writeSample(String metricName, Number value, String[] labelsAndValuesArray,
                     String[] additionalLabelsAndValues) {
        writeSamplePrefix(metricName, labelsAndValuesArray, additionalLabelsAndValues)
                .write("} ").write(value).write('\n');
    }

    void writeSample(String metricName, long value, String[] labelsAndValuesArray,
                     String[] additionalLabelsAndValues) {
        writeSamplePrefix(metricName, labelsAndValuesArray, additionalLabelsAndValues)
                .write("} ").write(value).write('\n');
    }

    void writeSample(String metricName, double value, String[] labelsAndValuesArray,
                     String[] additionalLabelsAndValues) {
        writeSamplePrefix(metricName, labelsAndValuesArray, additionalLabelsAndValues)
                .write("} ").write(value).write('\n');
    }

    void writeSample(String metricName, Number value, String[] labelsAndValuesArray,
                     String label, String labelValue) {
        writeSamplePrefix(metricName, labelsAndValuesArray, label, labelValue)
                .write("} ").write(value).write('\n');
    }

    void writeSample(String metricName, long value, String[] labelsAndValuesArray,
                     String label, String labelValue) {
        writeSamplePrefix(metricName, labelsAndValuesArray, label, labelValue)
                .write("} ").write(value).write('\n');
    }

    void writeSample(String metricName, double value, String[] labelsAndValuesArray,
                     String label, String labelValue) {
        writeSamplePrefix(metricName, labelsAndValuesArray, label, labelValue)
                .write("} ").write(value).write('\n');
    }

    private SimpleTextOutputStream writeSamplePrefix(String metricName, String[] labelsAndValuesArray) {
        SimpleTextOutputStream stream = initGaugeType(metricName);
        stream.write(metricName).write('{');
        writeLabels(stream, labelsAndValuesArray);
        return stream;
    }

    private SimpleTextOutputStream writeSamplePrefix(String metricName, String[] labelsAndValuesArray,
                                                     String[] additionalLabelsAndValues) {
        SimpleTextOutputStream stream = writeSamplePrefix(metricName, labelsAndValuesArray);
        if (additionalLabelsAndValues == null) {
            return stream;
        }
        if (labelsAndValuesArray.length != 0 && additionalLabelsAndValues.length != 0) {
            stream.write(',');
        }
        writeLabels(stream, additionalLabelsAndValues);
        return stream;
    }

    private SimpleTextOutputStream writeSamplePrefix(String metricName, String[] labelsAndValuesArray,
                                                     String label, String labelValue) {
        SimpleTextOutputStream stream = writeSamplePrefix(metricName, labelsAndValuesArray);
        if (labelsAndValuesArray.length != 0) {
            stream.write(',');
        }
        writeLabel(stream, label, labelValue);
        return stream;
    }

    private static void writeLabels(SimpleTextOutputStream stream, String[] labelsAndValuesArray) {
        for (int i = 0; i < labelsAndValuesArray.length; i += 2) {
            writeLabel(stream, labelsAndValuesArray[i], labelsAndValuesArray[i + 1]);
            if (i + 2 != labelsAndValuesArray.length) {
                stream.write(',');
            }
        }
    }

    private static void writeLabel(SimpleTextOutputStream stream, String label, String labelValue) {
        if (labelValue != null && labelValue.indexOf('"') > -1) {
            labelValue = labelValue.replace("\"", "\\\"");
        }
        stream.write(label).write("=\"").write(labelValue).write('\"');
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
