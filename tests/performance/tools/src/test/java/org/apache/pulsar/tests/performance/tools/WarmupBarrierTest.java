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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class WarmupBarrierTest {
    @Test
    public void waitsForEveryApplicationInTheRound() throws Exception {
        Path directory = Files.createTempDirectory("warmup-barrier-test");
        try {
            WarmupBarrier.markApplicationComplete(directory, "old-run", 2, 0);
            WarmupBarrier.markApplicationComplete(directory, "old-run", 2, 1);
            // Neither a previous run nor a different round can release the current barrier.
            WarmupBarrier.markApplicationComplete(directory, "current-run", 1, 1);
            WarmupBarrier.markApplicationComplete(directory, "current-run", 2, 0);
            long incompleteDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
            assertThatThrownBy(() -> WarmupBarrier.awaitApplications(directory, "current-run", 2, 2,
                    incompleteDeadline)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Timed out");
            WarmupBarrier.markApplicationComplete(directory, "current-run", 2, 1);

            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            assertThatCode(() -> WarmupBarrier.awaitApplications(directory, "current-run", 2, 2, deadlineNanos))
                    .doesNotThrowAnyException();
        } finally {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }
}
