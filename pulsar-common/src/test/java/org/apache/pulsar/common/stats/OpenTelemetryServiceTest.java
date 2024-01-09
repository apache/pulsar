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
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;
import org.testng.annotations.Test;

public class OpenTelemetryServiceTest {

    @Test
    public void testA() throws Exception {
        Object ignore = LoggingMetricExporter.class;
        Map<String, String> extraProperties = new HashMap<>();
        extraProperties.put("otel.metrics.exporter", "test");
        extraProperties.put("otel.metric.export.interval", "1000");

        @Cleanup
        OpenTelemetryService ots = new OpenTelemetryService("clusterName", extraProperties);

        Meter meter = ots.getMeter("pulsar.test");
        LongCounter longCounter = meter.counterBuilder("counter.A").build();
        longCounter.add(1);

        Thread.sleep(10000);
    }
}
