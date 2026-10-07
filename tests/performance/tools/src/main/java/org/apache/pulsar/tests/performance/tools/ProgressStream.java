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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.zip.Deflater;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Streams a workload's progress to the launcher as newline-delimited JSON, so that the launcher can report the run
 * as it goes, as pulsar-perf does. {@code GET /progress?intervalMillis=<ms>} answers with a line per interval until
 * the workload finishes, and a last line with {@code "final": true} then.
 *
 * <p>Each line has the workload's status, which the command fills in, such as its phase and cumulative message
 * counts, and the latencies recorded in the interval as a compressed, base64-encoded HdrHistogram of microseconds,
 * so that the launcher can merge the intervals of every application. Each stream records the latencies into its
 * own recorder, which leaves the latency logs untouched; with several latency logs, such as one per application, the
 * stream's intervals merge them.
 */
final class ProgressStream {
    static final String PATH = "/progress";
    static final long DEFAULT_INTERVAL_MILLIS = 1000;
    private static final long MIN_INTERVAL_MILLIS = 100;
    private static final long MAX_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<HdrLatencyRecorder> latencies;
    private final Consumer<ObjectNode> status;
    private final CountDownLatch finished = new CountDownLatch(1);

    /**
     * @param latency the latencies to report
     * @param status fills in a line's status fields; it is called from the stream's threads
     */
    ProgressStream(HdrLatencyRecorder latency, Consumer<ObjectNode> status) {
        this(List.of(latency), status);
    }

    /**
     * @param latencies the latencies to report, merged
     * @param status fills in a line's status fields; it is called from the stream's threads
     */
    ProgressStream(List<HdrLatencyRecorder> latencies, Consumer<ObjectNode> status) {
        if (latencies.isEmpty()) {
            throw new IllegalArgumentException("A progress stream needs a latency log");
        }
        this.latencies = List.copyOf(latencies);
        this.status = status;
    }

    /** Ends the open streams, each with a last line. */
    void finish() {
        finished.countDown();
    }

    void handle(HttpExchange exchange) throws IOException {
        long intervalMillis = DEFAULT_INTERVAL_MILLIS;
        String query = exchange.getRequestURI().getQuery();
        if (query != null && query.startsWith("intervalMillis=")) {
            try {
                intervalMillis = Math.min(MAX_INTERVAL_MILLIS, Math.max(MIN_INTERVAL_MILLIS,
                        Long.parseLong(query.substring("intervalMillis=".length()))));
            } catch (NumberFormatException e) {
                exchange.sendResponseHeaders(400, -1);
                exchange.close();
                return;
            }
        }
        Recorder recorder = latencies.get(0).addProgressRecorder();
        for (HdrLatencyRecorder latency : latencies.subList(1, latencies.size())) {
            latency.addProgressRecorder(recorder);
        }
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            // Chunked, as the stream has no length
            exchange.sendResponseHeaders(200, 0);
            OutputStream body = exchange.getResponseBody();
            Histogram interval = null;
            boolean last = false;
            while (!last) {
                try {
                    last = finished.await(intervalMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // The server is stopping
                    last = true;
                }
                interval = recorder.getIntervalHistogram(interval);
                body.write(line(interval, last));
                body.flush();
            }
        } catch (IOException e) {
            // The launcher went away; it reconnects when it wants the progress again
        } finally {
            for (HdrLatencyRecorder latency : latencies) {
                latency.removeProgressRecorder(recorder);
            }
        }
    }

    private byte[] line(Histogram interval, boolean last) {
        ObjectNode line = MAPPER.createObjectNode();
        line.put("epochMs", System.currentTimeMillis());
        status.accept(line);
        ObjectNode latencyNode = line.putObject("latency");
        latencyNode.put("count", interval.getTotalCount());
        ByteBuffer buffer = ByteBuffer.allocate(interval.getNeededByteBufferCapacity());
        int length = interval.encodeIntoCompressedByteBuffer(buffer, Deflater.BEST_SPEED);
        latencyNode.put("histogram", Base64.getEncoder().encodeToString(
                Arrays.copyOf(buffer.array(), length)));
        if (last) {
            line.put("final", true);
        }
        return (line + "\n").getBytes(StandardCharsets.UTF_8);
    }
}
