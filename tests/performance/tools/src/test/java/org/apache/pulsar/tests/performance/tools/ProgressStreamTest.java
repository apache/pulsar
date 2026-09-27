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
package org.apache.pulsar.tests.performance.tools;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;
import org.testng.annotations.Test;

public class ProgressStreamTest {
    private static final long MAX_LATENCY_MICROS = TimeUnit.HOURS.toMicros(1);

    private final ObjectMapper mapper = new ObjectMapper();

    @Test(timeOut = 30_000)
    public void streamsStatusAndIntervalLatenciesUntilTheLastLine() throws Exception {
        Path log = Files.createTempFile("progress-latency", ".hdr");
        try (MeasurementControl control = MeasurementControl.start(0)) {
            HdrLatencyRecorder latency = new HdrLatencyRecorder(log, MAX_LATENCY_MICROS);
            ProgressStream progress = new ProgressStream(latency, line -> line.put("phase", "warmup"));
            control.serveProgress(progress);
            HttpResponse<Stream<String>> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + control.port() + ProgressStream.PATH
                                    + "?intervalMillis=100")).GET().build(),
                    HttpResponse.BodyHandlers.ofLines());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("Content-Type")).hasValue("application/x-ndjson");
            try (Stream<String> lines = response.body()) {
                Iterator<String> iterator = lines.iterator();
                JsonNode first = mapper.readTree(iterator.next());
                assertThat(first.path("phase").asText()).isEqualTo("warmup");
                assertThat(first.has("final")).isFalse();

                // Warmup latencies reach the stream too
                latency.recordMillis(2, false);
                latency.recordMillis(4, true);
                long streamed = 0;
                Histogram merged = new Histogram(3);
                while (streamed < 2) {
                    JsonNode line = mapper.readTree(iterator.next());
                    streamed += line.path("latency").path("count").asLong();
                    merged.add(Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(
                            Base64.getDecoder().decode(line.path("latency").path("histogram").asText())), 0));
                }
                assertThat(merged.getTotalCount()).isEqualTo(2);
                assertThat(merged.getMinValue()).isBetween(1_999L, 2_001L);
                assertThat(merged.getMaxValue()).isBetween(3_999L, 4_003L);

                progress.finish();
                JsonNode last = null;
                while (iterator.hasNext()) {
                    last = mapper.readTree(iterator.next());
                }
                assertThat(last).isNotNull();
                assertThat(last.path("final").asBoolean()).isTrue();
            }
            latency.close();
        } finally {
            Files.deleteIfExists(log);
        }
    }

    @Test(timeOut = 30_000)
    public void mergesTheLatenciesOfSeveralLogs() throws Exception {
        Path firstLog = Files.createTempFile("progress-latency", ".hdr");
        Path secondLog = Files.createTempFile("progress-latency", ".hdr");
        try (MeasurementControl control = MeasurementControl.start(0)) {
            HdrLatencyRecorder first = new HdrLatencyRecorder(firstLog, MAX_LATENCY_MICROS);
            HdrLatencyRecorder second = new HdrLatencyRecorder(secondLog, MAX_LATENCY_MICROS);
            ProgressStream progress = new ProgressStream(List.of(first, second), line -> { });
            control.serveProgress(progress);
            HttpResponse<Stream<String>> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + control.port() + ProgressStream.PATH
                                    + "?intervalMillis=100")).GET().build(),
                    HttpResponse.BodyHandlers.ofLines());
            try (Stream<String> lines = response.body()) {
                Iterator<String> iterator = lines.iterator();
                // The first line is written once the stream has registered its recorder with both logs
                mapper.readTree(iterator.next());
                first.recordMillis(2, true);
                second.recordMillis(4, true);
                long streamed = 0;
                Histogram merged = new Histogram(3);
                while (streamed < 2) {
                    JsonNode line = mapper.readTree(iterator.next());
                    streamed += line.path("latency").path("count").asLong();
                    merged.add(Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(
                            Base64.getDecoder().decode(line.path("latency").path("histogram").asText())), 0));
                }
                assertThat(merged.getTotalCount()).isEqualTo(2);
                assertThat(merged.getMinValue()).isBetween(1_999L, 2_001L);
                assertThat(merged.getMaxValue()).isBetween(3_999L, 4_003L);
                progress.finish();
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
            first.close();
            second.close();
        } finally {
            Files.deleteIfExists(firstLog);
            Files.deleteIfExists(secondLog);
        }
    }
}
