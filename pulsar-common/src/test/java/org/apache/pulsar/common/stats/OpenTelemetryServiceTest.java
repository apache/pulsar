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
package org.apache.pulsar.common.stats;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class OpenTelemetryServiceTest {

    public static final Map<String, Consumer<MetricData>> metricConsumers = new ConcurrentHashMap<>();

    @Test
    public void testA() throws Exception {
        Consumer<MetricData> consumer = (Consumer<MetricData>) Mockito.mock(Consumer.class);
        String consumerUuid = TestMetricProvider.registerMetricConsumer(consumer);

        Map<String, String> extraProperties = new HashMap<>();
        extraProperties.put("otel.metrics.exporter", "test");
        extraProperties.put("otel.metric.export.interval", "100");
        extraProperties.put(TestMetricProvider.METRIC_CONSUMER_CONFIG_KEY, consumerUuid);

        @Cleanup
        OpenTelemetryService ots = new OpenTelemetryService("clusterName", extraProperties);

        Meter meter = ots.getMeter("pulsar.test");
        LongCounter longCounter = meter.counterBuilder("counter.A").build();
        longCounter.add(1);

        Mockito.verify(consumer, Mockito.timeout(1000).atLeastOnce()).
                accept(Mockito.argThat((MetricData md) -> "counter.A".equals(md.getName())));
    }
}
