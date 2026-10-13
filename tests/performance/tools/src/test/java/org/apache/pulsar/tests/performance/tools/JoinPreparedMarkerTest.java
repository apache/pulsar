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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class JoinPreparedMarkerTest {
    @Test
    public void lateApplicationsAwaitTheLauncherPreparingTheirJoin() throws Exception {
        Path directory = Files.createTempDirectory("coordination");
        CompletableFuture<Map<Integer, Long>> awaited = CompletableFuture.supplyAsync(() -> {
            try {
                return JoinPreparedMarker.await(directory, "run-1", System.nanoTime() + TimeUnit.SECONDS.toNanos(10));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(50);
        assertThat(awaited).isNotDone();
        // the messages that each application that reset its subscription expects
        JoinPreparedMarker.mark(directory, "run-1", Map.of(2, 250L, 4, 1_000_000L));
        assertThat(awaited.get(10, TimeUnit.SECONDS)).containsOnly(Map.entry(2, 250L), Map.entry(4, 1_000_000L));
        // a join prepared without resets expects no backlog
        JoinPreparedMarker.mark(directory, "run-3");
        assertThat(JoinPreparedMarker.await(directory, "run-3", System.nanoTime() + TimeUnit.SECONDS.toNanos(10)))
                .isEmpty();
        // another run's marker isn't this run's
        assertThatThrownBy(() -> JoinPreparedMarker.await(directory, "run-2",
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100)))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("Timed out waiting for the launcher to prepare the join");
    }
}
