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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.functions.api.metrics.Summary;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PrometheusSummaryBuilderTest {

    private static final String METRIC_PREFIX = "prefix";
    private static final String[] SYSTEM_LABEL_NAMES = new String[]{"sysLabel1", "sysLabel2"};
    private static final String[] SYSTEM_LABEL_VALUES = new String[]{"foo", "bar"};

    private CollectorRegistry mockRegistry;
    private PrometheusSummaryBuilder builder;
    private ArgumentCaptor<Collector> collectorCaptor;
    private List<Summary.Quantile> quantiles;

    @BeforeMethod
    public void setup() {
        mockRegistry = mock(CollectorRegistry.class);
        collectorCaptor = ArgumentCaptor.forClass(Collector.class);
        builder = new PrometheusSummaryBuilder(mockRegistry, METRIC_PREFIX, SYSTEM_LABEL_NAMES, SYSTEM_LABEL_VALUES);
        quantiles = new ArrayList<>();
        quantiles.add(Summary.Quantile.of(0.5, 0.01));
        quantiles.add(Summary.Quantile.of(0.9, 0.01));
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

    @Test
    public void testDefaultSummaryRegistration() {
        builder.name("name").labelNames("label1").labels("labelValue").help("help");
        for (Summary.Quantile quantile: quantiles) {
            builder.quantile(quantile.getQuantile(), quantile.getError());
        }
        builder.register();

        verify(mockRegistry).register(collectorCaptor.capture());
        Collector collector = collectorCaptor.getValue();
        assertEquals(collector.getClass(), PrometheusSummaryBuilder.SummaryCollector.class);
        PrometheusSummaryBuilder.SummaryCollector promHistogram =
                (PrometheusSummaryBuilder.SummaryCollector) collector;

        List<Collector.MetricFamilySamples> samples = promHistogram.collect();
        Collector.MetricFamilySamples sample = samples.get(0);
        assertEquals(sample.name, METRIC_PREFIX + "name");
        assertEquals(sample.help, "help");
        List<Collector.MetricFamilySamples.Sample> sampleList = sample.samples;

        // should have metric value for each bucket with a count and a sum
        assertEquals(sampleList.size(), quantiles.size() + 2);

        // should find metric value for each quantile with additional quantile label
        for (Summary.Quantile quantile: quantiles) {
            assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name") &&
                    CollectionUtils.isEqualCollection(s.labelNames,
                            Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES,
                                    new String[]{"label1", "quantile"}, String.class))) &&
                    CollectionUtils.isEqualCollection(s.labelValues,
                            Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES,
                                    new String[]{"labelValue", String.valueOf(quantile.getQuantile())},
                                    String.class)))));
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
    public void testProvidedSummaryRegistration() {
        Summary summary = mock(Summary.class);
        double summarySum = 123;
        double summaryCount = 321;
        when(summary.getSum()).thenReturn(summarySum);
        when(summary.getCount()).thenReturn(summaryCount);
        when(summary.getQuantiles()).thenReturn(quantiles);
        Map<Double, Double> quantileValues = quantiles.stream()
                .collect(Collectors.toMap(Summary.Quantile::getQuantile, q -> q.getQuantile() + 1));
        when(summary.getQuantileValues()).thenReturn(quantileValues);

        builder.name("name").labelNames("label1").labels("labelValue").summary(summary).help("help")
                .register();

        verify(mockRegistry).register(collectorCaptor.capture());
        Collector collector = collectorCaptor.getValue();
        assertEquals(collector.getClass(), PrometheusSummaryBuilder.SummaryCollector.class);
        PrometheusSummaryBuilder.SummaryCollector promHistogram =
                (PrometheusSummaryBuilder.SummaryCollector) collector;

        List<Collector.MetricFamilySamples> samples = promHistogram.collect();
        Collector.MetricFamilySamples sample = samples.get(0);
        assertEquals(sample.name, METRIC_PREFIX + "name");
        assertEquals(sample.help, "help");
        List<Collector.MetricFamilySamples.Sample> sampleList = sample.samples;

        // should have metric value for each bucket with a count and a sum
        assertEquals(sampleList.size(), quantiles.size() + 2);

        // should find metric value for each quantile with additional quantile label
        for (Summary.Quantile quantile: quantiles) {
            double expectedQuantileValue = quantileValues.get(quantile.getQuantile());
            assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name") &&
                    s.value == expectedQuantileValue &&
                    CollectionUtils.isEqualCollection(s.labelNames,
                            Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES,
                                    new String[]{"label1", "quantile"}, String.class))) &&
                    CollectionUtils.isEqualCollection(s.labelValues,
                            Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES,
                                    new String[]{"labelValue", String.valueOf(quantile.getQuantile())},
                                    String.class)))));
        }

        // should have _count metric value
        assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name" + "_count") &&
                s.value == summaryCount &&
                CollectionUtils.isEqualCollection(s.labelNames,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES, "label1"))) &&
                CollectionUtils.isEqualCollection(s.labelValues,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES, "labelValue")))));

        // should have _sum metric value
        assertTrue(sampleList.stream().anyMatch(s -> s.name.equals(METRIC_PREFIX + "name" + "_sum") &&
                s.value == summarySum &&
                CollectionUtils.isEqualCollection(s.labelNames,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES, "label1"))) &&
                CollectionUtils.isEqualCollection(s.labelValues,
                        Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES, "labelValue")))));
    }

}
