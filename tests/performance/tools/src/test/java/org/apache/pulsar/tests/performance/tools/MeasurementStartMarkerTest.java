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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class MeasurementStartMarkerTest {
    @Test
    public void applicationsAwaitTheGatewaysMeasurementStart() throws Exception {
        Path directory = Files.createTempDirectory("coordination");
        CompletableFuture<Long> awaited = CompletableFuture.supplyAsync(() -> {
            try {
                return MeasurementStartMarker.await(directory, "run-1",
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(10));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(50);
        assertThat(awaited).isNotDone();
        MeasurementStartMarker.mark(directory, "run-1", 1_790_000_000_000L);
        assertThat(awaited.get(10, TimeUnit.SECONDS)).isEqualTo(1_790_000_000_000L);
        // another run's marker isn't this run's
        assertThatThrownBy(() -> MeasurementStartMarker.await(directory, "run-2",
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100)))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("Timed out waiting for the measurement to start");
    }
}
