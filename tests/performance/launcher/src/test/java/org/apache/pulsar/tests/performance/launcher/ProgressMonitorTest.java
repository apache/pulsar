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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.Deflater;
import org.HdrHistogram.Histogram;
import org.testng.annotations.Test;

public class ProgressMonitorTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void reportsThroughputLatencyAndBacklogOfTheProducerAndTheApplications() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);
        TopicStatsSampler.Backlog backlog = new TopicStatsSampler.Backlog(System.currentTimeMillis(), 1_500, 1_000);
        try (ProgressMonitor monitor = new ProgressMonitor(mapper, out, 125_000, 2, () -> backlog)) {
            ObjectNode producer = line("producer", 1_000, 2_000);
            producer.put("phase", "warmup").put("sent", 1_000).put("pending", 7).put("messageCount", 4_000)
                    .put("warmupRound", 0).put("warmupRounds", 1);
            monitor.accept("producer", producer);
            // Two lines of the applications' container, which sums its two applications' counts and merges their
            // latencies
            for (long latencyMicros : new long[] {10_000, 20_000}) {
                ObjectNode consumer = line("consumer", 500, latencyMicros);
                consumer.put("phase", "receiving").put("applications", 2).put("finishedApplications", 1)
                        .put("received", 1_000).put("messageCount", 8_000);
                monitor.accept("applications", consumer);
            }
            monitor.report();
        }
        String[] lines = output.toString(StandardCharsets.UTF_8).split("\n");
        assertThat(lines).hasSize(2);
        assertThat(lines[0]).contains("warmup round 1/1", "Produced: 1,000 msg of 4,000 (25%)", "pending: 7",
                "Latency: mean: 2.000 ms");
        assertThat(lines[1]).contains("Received: 1,000 msg of 8,000 (12%)",
                "backlog: 1,500 msg (max per application: 1,000)", "finished applications: 1/2", "med: 10.0",
                "Max: 20.0");
    }

    @Test
    public void saysHowOldAnOldBacklogSampleIs() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);
        TopicStatsSampler.Backlog backlog =
                new TopicStatsSampler.Backlog(System.currentTimeMillis() - 30_000, 1_241, 73);
        try (ProgressMonitor monitor = new ProgressMonitor(mapper, out, 64, 1, () -> backlog)) {
            monitor.report();
        }
        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("backlog: 1,241 msg (max per application: 73, sampled 30 s ago)");
    }

    private ObjectNode line(String role, int count, long latencyMicros) {
        Histogram histogram = new Histogram(3);
        histogram.recordValueWithCount(latencyMicros, count);
        ByteBuffer buffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
        int length = histogram.encodeIntoCompressedByteBuffer(buffer, Deflater.BEST_SPEED);
        ObjectNode line = mapper.createObjectNode().put("role", role).put("epochMs", System.currentTimeMillis());
        line.putObject("latency").put("count", count).put("histogram",
                Base64.getEncoder().encodeToString(Arrays.copyOf(buffer.array(), length)));
        return line;
    }
}
