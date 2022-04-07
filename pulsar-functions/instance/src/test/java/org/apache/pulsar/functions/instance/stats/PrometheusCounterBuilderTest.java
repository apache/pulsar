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
import com.google.common.collect.ObjectArrays;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.util.Arrays;
import java.util.List;
import org.apache.pulsar.functions.api.metrics.Counter;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PrometheusCounterBuilderTest {

    private static final String METRIC_PREFIX = "prefix";
    private static final String[] SYSTEM_LABEL_NAMES = new String[]{"sysLabel1", "sysLabel2"};
    private static final String[] SYSTEM_LABEL_VALUES = new String[]{"foo", "bar"};

    private CollectorRegistry mockRegistry;
    private PrometheusCounterBuilder builder;
    private ArgumentCaptor<Collector> collectorCaptor;

    @BeforeMethod
    public void setup() {
        mockRegistry = mock(CollectorRegistry.class);
        collectorCaptor = ArgumentCaptor.forClass(Collector.class);
        builder = new PrometheusCounterBuilder(mockRegistry, METRIC_PREFIX, SYSTEM_LABEL_NAMES, SYSTEM_LABEL_VALUES);
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
    public void testDefaultCounterRegistration() {
        builder.name("name").labelNames("label1").labels("labelValue").help("help").register();

        verify(mockRegistry).register(collectorCaptor.capture());
        Collector collector = collectorCaptor.getValue();
        assertEquals(collector.getClass(), io.prometheus.client.Counter.class);
        io.prometheus.client.Counter promCounter = (io.prometheus.client.Counter) collector;

        List<Collector.MetricFamilySamples> samples = promCounter.collect();
        Collector.MetricFamilySamples sample = samples.get(0);
        assertEquals(sample.name, METRIC_PREFIX + "name");
        assertEquals(sample.help, "help");
        List<Collector.MetricFamilySamples.Sample> sampleList = sample.samples;
        sampleList.forEach(s -> {
            assertEquals(s.name, METRIC_PREFIX + "name");
            assertEquals(s.labelNames, Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES, "label1")));
            assertEquals(s.labelValues, Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES, "labelValue")));
        });
    }

    @Test
    public void testProvidedCounterRegistration() {
        double counterValue = 123;
        Counter mockCounter = mock(Counter.class);
        when(mockCounter.get()).thenReturn(counterValue);

        builder.name("name").labelNames("label1").labels("labelValue").help("help")
                .counter(mockCounter).register();

        verify(mockRegistry).register(collectorCaptor.capture());
        Collector collector = collectorCaptor.getValue();
        assertEquals(collector.getClass(), io.prometheus.client.Counter.class);
        io.prometheus.client.Counter promCounter = (io.prometheus.client.Counter) collector;

        List<Collector.MetricFamilySamples> samples = promCounter.collect();
        Collector.MetricFamilySamples sample = samples.get(0);
        assertEquals(sample.name, METRIC_PREFIX + "name");
        assertEquals(sample.help, "help");
        List<Collector.MetricFamilySamples.Sample> sampleList = sample.samples;
        sampleList.forEach(s -> {
            assertEquals(s.name, METRIC_PREFIX + "name");
            assertEquals(s.labelNames, Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_NAMES, "label1")));
            assertEquals(s.labelValues, Arrays.asList(ObjectArrays.concat(SYSTEM_LABEL_VALUES, "labelValue")));
            assertEquals(s.value, counterValue);
        });
    }
}
