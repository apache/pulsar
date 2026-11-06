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
package org.apache.pulsar.tests.performance.report;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.Recording;
import org.testng.annotations.Test;

public class NettyAllocatorEventsTest {
    private final ObjectMapper mapper = new ObjectMapper();

    // Stand-ins for Netty's events, with their names and fields, so that the test needs neither Netty nor its version
    @Name("io.netty.AllocateBuffer")
    static class AllocateBuffer extends Event {
        Class<?> allocatorType;
        int size;
        boolean direct;
        boolean chunkPooled;
        boolean chunkThreadLocal;
    }

    @Name("io.netty.FreeBuffer")
    static class FreeBuffer extends Event {
        Class<?> allocatorType;
        int size;
        boolean direct;
    }

    @Name("io.netty.ReallocateBuffer")
    static class ReallocateBuffer extends Event {
        Class<?> allocatorType;
        int size;
        boolean direct;
        int newCapacity;
    }

    @Name("io.netty.AllocateChunk")
    static class AllocateChunk extends Event {
        Class<?> allocatorType;
        int capacity;
        boolean direct;
        boolean pooled;
        boolean threadLocal;
    }

    // Stands for an allocator class, whose name without its package the summary shows
    static final class AdaptivePoolingAllocator {
    }

    @Test
    public void summarizesTheAllocatorEvents() throws Exception {
        Path directory = Files.createTempDirectory("netty-allocator-events");
        Path recording = directory.resolve("profile.measurement.jfr");
        try (Recording jfr = new Recording()) {
            jfr.enable(AllocateBuffer.class);
            jfr.enable(FreeBuffer.class);
            jfr.enable(ReallocateBuffer.class);
            jfr.enable(AllocateChunk.class);
            jfr.start();
            Thread thread = new Thread(() -> {
                allocate(128, true);
                allocate(128, true);
                allocate(8192, true);
                // Too large for the pooled chunks: a chunk of its own
                allocate(2_000_000, false);
                FreeBuffer free = new FreeBuffer();
                free.allocatorType = AdaptivePoolingAllocator.class;
                free.size = 128;
                free.direct = true;
                free.commit();
                ReallocateBuffer reallocate = new ReallocateBuffer();
                reallocate.allocatorType = AdaptivePoolingAllocator.class;
                reallocate.size = 128;
                reallocate.direct = true;
                reallocate.newCapacity = 256;
                reallocate.commit();
                AllocateChunk chunk = new AllocateChunk();
                chunk.allocatorType = AdaptivePoolingAllocator.class;
                chunk.capacity = 4_194_304;
                chunk.direct = true;
                chunk.pooled = true;
                chunk.commit();
            }, "pulsar-io-3-7");
            thread.start();
            thread.join();
            jfr.stop();
            jfr.dump(recording);
        }

        Path output = NettyAllocatorEvents.write(recording, Duration.ofSeconds(2), 2, mapper);

        assertThat(output).isEqualTo(directory.resolve("profile.measurement.netty-allocator.json"));
        JsonNode summary = mapper.readTree(output.toFile());
        assertThat(summary.path("seconds").asDouble()).isEqualTo(2.0);
        assertThat(eventCount(summary, "io.netty.AllocateBuffer")).isEqualTo(4);
        assertThat(eventCount(summary, "io.netty.FreeBuffer")).isEqualTo(1);
        assertThat(eventCount(summary, "io.netty.ReallocateBuffer")).isEqualTo(1);
        assertThat(eventCount(summary, "io.netty.AllocateChunk")).isEqualTo(1);
        JsonNode oneOff = find(summary.path("bufferAllocations"), "chunk", "one-off");
        assertThat(oneOff.path("allocator").asText()).isEqualTo("NettyAllocatorEventsTest$AdaptivePoolingAllocator");
        assertThat(oneOff.path("count").asLong()).isEqualTo(1);
        assertThat(oneOff.path("bytes").asLong()).isEqualTo(2_000_000);
        assertThat(find(summary.path("bufferAllocations"), "chunk", "pooled").path("count").asLong()).isEqualTo(3);
        // 128 B twice, 8 KiB once and the 2 MB buffer above the largest size class
        assertThat(summary.path("bufferSizes").get(0).path("upToBytes").asLong()).isEqualTo(256);
        assertThat(summary.path("bufferSizes").get(0).path("count").asLong()).isEqualTo(2);
        assertThat(summary.path("bufferSizes").get(2).path("upToBytes").isNull()).isTrue();
        assertThat(summary.path("bufferSizes").get(2).path("oneOff").asLong()).isEqualTo(1);
        assertThat(summary.path("bufferAllocationsByThreadPool").get(0).path("threadPool").asText())
                .isEqualTo("pulsar-io");

        StringBuilder report = new StringBuilder();
        NettyAllocatorEvents.appendReport(report, summary, output.getFileName().toString());
        assertThat(report.toString())
                .contains("### Netty allocator events")
                .contains("| `io.netty.AllocateBuffer` | 4 | 2 | 2.000 |")
                .contains("| NettyAllocatorEventsTest$AdaptivePoolingAllocator | direct | one-off | shared | 1 |")
                .contains("| over 1 MiB | 1 | 25.0 % | 2.0 | 1 |")
                .contains("| pulsar-io | 4 |");
    }

    @Test
    public void writesAnEmptySummaryWithoutAllocatorEvents() throws Exception {
        Path recording = Files.createTempDirectory("netty-allocator-events").resolve("empty.jfr");
        try (Recording jfr = new Recording()) {
            jfr.start();
            jfr.stop();
            jfr.dump(recording);
        }

        Path output = NettyAllocatorEvents.write(recording, Duration.ofSeconds(2), 10, mapper);

        JsonNode summary = mapper.readTree(output.toFile());
        assertThat(summary.path("events")).isEmpty();
        StringBuilder report = new StringBuilder();
        NettyAllocatorEvents.appendReport(report, summary, output.getFileName().toString());
        assertThat(report.toString()).contains("The recording has no events of Netty's buffer allocators")
                .doesNotContain("| Event |");
    }

    @Test
    public void classifiesBufferSizes() {
        assertThat(NettyAllocatorEvents.sizeClass(64)).isZero();
        assertThat(NettyAllocatorEvents.sizeClass(65)).isEqualTo(1);
        assertThat(NettyAllocatorEvents.sizeClass(2_000_000)).isEqualTo(8);
    }

    private static void allocate(int size, boolean pooled) {
        AllocateBuffer event = new AllocateBuffer();
        event.allocatorType = AdaptivePoolingAllocator.class;
        event.size = size;
        event.direct = true;
        event.chunkPooled = pooled;
        event.chunkThreadLocal = false;
        event.commit();
    }

    private static long eventCount(JsonNode summary, String event) {
        return find(summary.path("events"), "event", event).path("count").asLong();
    }

    private static JsonNode find(JsonNode rows, String field, String value) {
        for (JsonNode row : rows) {
            if (value.equals(row.path(field).asText())) {
                return row;
            }
        }
        return MissingNode.getInstance();
    }
}
