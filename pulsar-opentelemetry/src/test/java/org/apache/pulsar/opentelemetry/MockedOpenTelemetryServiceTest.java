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
package org.apache.pulsar.opentelemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import lombok.Getter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MockedOpenTelemetryServiceTest {
    private MockedOpenTelemetryService server;

    @BeforeMethod
    public void setup() throws IOException {
        server = new MockedOpenTelemetryService();
        server.start();
    }

    @AfterMethod
    public void tearDown() {
        server.stop();
    }

    @Test
    public void testGetMetrics() throws InterruptedException {
        OtlpMetricsExporter client =
                new OtlpMetricsExporter("http://127.0.0.1:" + server.getPort(), 1L);
        LongCounter requestCounter = client.getMeter()
                .counterBuilder("request_counter")
                .setDescription("request counter")
                .setUnit("1")
                .build();
        DoubleHistogram requestLatency = client.getMeter()
                .histogramBuilder("request_time_sec")
                .setDescription("request latency")
                .setUnit("s")
                .build();
        double sum = 0;
        for (int i = 0; i < 10; i++) {
            requestCounter.add(1, Attributes.of(AttributeKey.stringKey("env"), "prod"));
            double latency = Math.random() * 100 / 1000;
            sum += latency;
            requestLatency.record(latency, Attributes.of(AttributeKey.stringKey("env"), "prod"));
        }
        Thread.sleep(2000);
        List<MockedOpenTelemetryService.StoredMetric> metrics = server.getMetricsDataStore().getAllMetric();
        for (MockedOpenTelemetryService.StoredMetric metric : metrics) {
            for (HistogramDataPoint point : metric.getMetric().getHistogram().getDataPointsList()) {
                Assert.assertEquals(point.getSum(), sum, 0.001);
            }
        }

    }

    @Getter
    public static class OtlpMetricsExporter {
        private final OpenTelemetrySdk openTelemetrySdk;
        private final Meter meter;
        private final MetricExporter exporter;
        private final MetricReader reader;

        public OtlpMetricsExporter(String endPoint, long duration) {
            this(endPoint, Duration.ofSeconds(duration));
        }

        public OtlpMetricsExporter(String endPoint, Duration duration) {
            exporter = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(endPoint)
                    .setTimeout(Duration.ofSeconds(10))
                    .build();
            reader = PeriodicMetricReader.builder(exporter)
                    .setInterval(duration)
                    .build();
            Resource resource = Resource.getDefault().merge(
                    Resource.builder()
                            .put("service.name", "custom-metrics-exporter")
                            .put("exporter.type", "otlp")
                            .build()
            );
            openTelemetrySdk = OpenTelemetrySdk.builder()
                    .setMeterProvider(
                            SdkMeterProvider.builder()
                                    .setResource(resource)
                                    .registerMetricReader(reader)
                                    .build())
                    .build();
            meter = openTelemetrySdk.getMeter("custom-metrics-exporter");
        }

        public void shutdown() {
            reader.shutdown();
            exporter.shutdown();
            openTelemetrySdk.close();
        }
    }
}
