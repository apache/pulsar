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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.testng.annotations.Test;

public class RandomSeeksTest {
    /** Records the operations, and fails a reset or sleeps in each as told. */
    private static final class FakeTopicOperations implements RandomSeeks.TopicOperations {
        private final List<String> operations = new ArrayList<>();
        private int failResetAt = -1;
        private long resetMillis;
        private int resets;

        @Override
        public void createSubscription(String topic) {
            operations.add("create " + topic);
        }

        @Override
        public void unload(String topic) {
            operations.add("unload " + topic);
        }

        @Override
        public void resetCursor(String topic, long publishTime) throws Exception {
            if (resets++ == failResetAt) {
                throw new IOException("reset failed");
            }
            Thread.sleep(resetMillis);
        }

        @Override
        public String peek(String topic) {
            return "12:" + resets + ":-1";
        }

        @Override
        public void deleteSubscription(String topic) {
            operations.add("delete " + topic);
        }
    }

    private static Path runOutput() throws IOException {
        return Files.createTempDirectory("random-seeks");
    }

    @Test
    public void seeksAndRemovesItsSubscriptions() throws Exception {
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        Path output = runOutput();
        RandomSeeks.run(topicOperations, List.of("t-0", "t-1"), 1_000, 2_000, new SeekSettings(20, 1, true, true, 60),
                output, new CompletableFuture<>(), line -> { });
        List<String> lines = Files.readAllLines(output.resolve(RunReport.SEEKS_FILE));
        assertThat(lines).hasSize(21);
        assertThat(lines.get(0)).isEqualTo(RunReport.SEEKS_HEADER);
        for (String line : lines.subList(1, lines.size())) {
            String[] fields = line.split(",");
            assertThat(Long.parseLong(fields[4])).isBetween(1_000L, 2_000L);
            assertThat(fields[7]).startsWith("12:");
        }
        assertThat(topicOperations.operations).containsExactly("create t-0", "create t-1", "unload t-0", "unload t-1",
                "delete t-0", "delete t-1");
    }

    @Test
    public void removesItsSubscriptionsWhenASeekFails() throws Exception {
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        topicOperations.failResetAt = 3;
        assertThatThrownBy(() -> RandomSeeks.run(topicOperations, List.of("t-0", "t-1"), 1_000, 2_000,
                new SeekSettings(20, 1, false, false, 60), runOutput(), new CompletableFuture<>(), line -> { }))
                .hasMessage("reset failed");
        assertThat(topicOperations.operations).containsExactly("create t-0", "create t-1", "delete t-0", "delete t-1");
    }

    @Test
    public void stopsAtItsTimeout() throws Exception {
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        topicOperations.resetMillis = 400;
        assertThatThrownBy(() -> RandomSeeks.run(topicOperations, List.of("t-0"), 1_000, 2_000,
                new SeekSettings(100, 1, false, false, 1), runOutput(), new CompletableFuture<>(), line -> { }))
                .isInstanceOf(TimeoutException.class).hasMessageContaining("within offload.seeks.timeoutSeconds");
        assertThat(topicOperations.resets).isLessThan(10);
        assertThat(topicOperations.operations).contains("delete t-0");
    }

    @Test
    public void failsWhenTheLastSeekEndsAfterTheTimeout() throws Exception {
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        topicOperations.resetMillis = 1_500;
        assertThatThrownBy(() -> RandomSeeks.run(topicOperations, List.of("t-0"), 1_000, 2_000,
                new SeekSettings(1, 1, false, false, 1), runOutput(), new CompletableFuture<>(), line -> { }))
                .isInstanceOf(TimeoutException.class);
        assertThat(topicOperations.operations).contains("delete t-0");
    }

    @Test
    public void stopsWhenTheRunFails() throws Exception {
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        CompletableFuture<Void> runFailure = new CompletableFuture<>();
        runFailure.completeExceptionally(new IllegalStateException("network impairment failed"));
        assertThatThrownBy(() -> RandomSeeks.run(topicOperations, List.of("t-0"), 1_000, 2_000,
                new SeekSettings(100, 1, false, false, 60), runOutput(), runFailure, line -> { }))
                .hasMessageContaining("The run failed during the seeks")
                .hasRootCauseMessage("network impairment failed");
        // it stops before it creates anything
        assertThat(topicOperations.resets).isZero();
        assertThat(topicOperations.operations).isEmpty();
    }
}
