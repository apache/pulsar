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

package org.apache.pulsar.functions.instance.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.ObjectArrays;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.functions.api.metrics.Histogram;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PrometheusHistogramBuilderTest {

    private static final String METRIC_PREFIX = "prefix";
    private static final String[] SYSTEM_LABEL_NAMES = new String[]{"sysLabel1", "sysLabel2"};
    private static final String[] SYSTEM_LABEL_VALUES = new String[]{"foo", "bar"};
    private static final double[] BUCKETS = new double[]{1, 2, 3};

    private CollectorRegistry mockRegistry;
    private PrometheusHistogramBuilder builder;
    private ArgumentCaptor<Collector> collectorCaptor;

    @BeforeMethod
    public void setup() {
        mockRegistry = mock(CollectorRegistry.class);
        collectorCaptor = ArgumentCaptor.forClass(Collector.class);
        builder = new PrometheusHistogramBuilder(mockRegistry, METRIC_PREFIX, SYSTEM_LABEL_NAMES, SYSTEM_LABEL_VALUES);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMissingName() {
        builder.labelNames().labels().help("help").register();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMissingHelpMsg() {
        builder.name("name").labelNames().labels().register();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLabelNamesLabelValuesMismatch() {
        builder.name("name").labelNames("label1", "label2").labels("foo").help("help").register();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLabelNamesLabelValuesMismatch2() {
        builder.name("name").labelNames("label1").labels("foo", "bar").help("help").register();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMissingLabelNamesButLabelValuesProvided() {
        builder.name("name").labelNames().labels("foo", "bar").help("help").register();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLabelNamesProvidedButMissingLabelValues() {
        builder.name("name").labelNames("label1", "label2").labels().help("help").register();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMissingBuckets() {
        builder.name("name").labelNames("label1").labels("labelValue").help("help").register();
    }

    @Test
    public void testDefaultHistogramRegistration() {
        builder.name("name").labelNames("label1").labels("labelValue").help("help")
                .buckets(BUCKETS)
                .register();

        verify(mockRegistry).register(collectorCaptor.capture());
        Collector collector = collectorCaptor.getValue();
        assertEquals(collector.getClass(), PrometheusHistogramBuilder.HistogramCollector.class);
        PrometheusHistogramBuilder.HistogramCollector promHistogram =
                (PrometheusHistogramBuilder.HistogramCollector) collector;

        List<Collector.MetricFamilySamples> samples = promHistogram.collect();
        Collector.MetricFamilySamples sample = samples.get(0);
        assertEquals(sample.name, METRIC_PREFIX + "name");
        assertEquals(sample.help, "help");
        List<Collector.MetricFamilySamples.Sample> sampleList = sample.samples;

        // should have metric value for each bucket with a count and a sum
        assertEquals(sampleList.size(), BUCKETS.length + 2);

        // should find metric value for each bucket, with _bucket suffix for the name and le label
        for (double bucket: BUCKETS) {
            assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name" + "_bucket") &&
                    CollectionUtils.isEqualCollection(s.labelNames,
                            Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES,
                                    new String[]{"label1", "le"}, String.class))) &&
                    CollectionUtils.isEqualCollection(s.labelValues,
                            Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES,
                                    new String[]{"labelValue", String.valueOf(bucket)}, String.class)))));

        }

        // should have _count metric value
        assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name" + "_count") &&
                        CollectionUtils.isEqualCollection(s.labelNames,
                                Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES, "label1"))) &&
                        CollectionUtils.isEqualCollection(s.labelValues,
                                Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES, "labelValue")))));

        // should have _sum metric value
        assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name" + "_sum") &&
                CollectionUtils.isEqualCollection(s.labelNames,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES, "label1"))) &&
                CollectionUtils.isEqualCollection(s.labelValues,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES, "labelValue")))));
    }

    @Test
    public void testProvidedHistogramRegistration() {
        Histogram histogram = mock(Histogram.class);
        double histogramCount = 123;
        double histogramSum = 321;
        double[] histogramBucketValues = Arrays.stream(BUCKETS).map(b -> b+1).toArray();
        when(histogram.getBuckets()).thenReturn(BUCKETS);
        when(histogram.getCount()).thenReturn(histogramCount);
        when(histogram.getSum()).thenReturn(histogramSum);
        when(histogram.getValues()).thenReturn(histogramBucketValues);

        builder.name("name").labelNames("label1").labels("labelValue").help("help")
                .histogram(histogram)
                .register();

        verify(mockRegistry).register(collectorCaptor.capture());
        Collector collector = collectorCaptor.getValue();
        assertEquals(collector.getClass(), PrometheusHistogramBuilder.HistogramCollector.class);
        PrometheusHistogramBuilder.HistogramCollector promHistogram =
                (PrometheusHistogramBuilder.HistogramCollector) collector;

        List<Collector.MetricFamilySamples> samples = promHistogram.collect();
        Collector.MetricFamilySamples sample = samples.get(0);
        assertEquals(sample.name, METRIC_PREFIX + "name");
        assertEquals(sample.help, "help");
        List<Collector.MetricFamilySamples.Sample> sampleList = sample.samples;

        // should have metric value for each bucket with a count and a sum
        assertEquals(sampleList.size(), BUCKETS.length + 2);

        // should find metric value for each bucket, with _bucket suffix for the name and le label
        for (int i = 0; i < BUCKETS.length; i++) {
            double expectedBucket = BUCKETS[i];
            double expectedBucketValue = histogramBucketValues[i];
            assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name" + "_bucket") &&
                    s.value == expectedBucketValue &&
                    CollectionUtils.isEqualCollection(s.labelNames,
                            Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES,
                                    new String[]{"label1", "le"}, String.class))) &&
                    CollectionUtils.isEqualCollection(s.labelValues,
                            Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES,
                                    new String[]{"labelValue", String.valueOf(expectedBucket)}, String.class)))));

        }

        // should have _count metric value
        assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name" + "_count") &&
                s.value == histogramCount &&
                CollectionUtils.isEqualCollection(s.labelNames,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES, "label1"))) &&
                CollectionUtils.isEqualCollection(s.labelValues,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES, "labelValue")))));

        // should have _sum metric value
        assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name" + "_sum") &&
                s.value == histogramSum &&
                CollectionUtils.isEqualCollection(s.labelNames,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES, "label1"))) &&
                CollectionUtils.isEqualCollection(s.labelValues,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES, "labelValue")))));
    }



}
