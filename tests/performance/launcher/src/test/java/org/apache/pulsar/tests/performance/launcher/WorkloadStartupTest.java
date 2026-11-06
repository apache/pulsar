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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testcontainers.containers.ContainerLaunchException;
import org.testng.annotations.Test;

public class WorkloadStartupTest {
    @Test
    public void showsEachNewProgressLineOnceAndFindsTheReadyLine() {
        List<String> shown = new ArrayList<>();
        WorkloadStartup startup = new WorkloadStartup(".*READY applications=.*", shown::add);

        assertThat(startup.accept("SLF4J(W): No SLF4J providers were found.\n")).isEqualTo(WorkloadStartup.Line.OTHER);
        assertThat(startup.accept("PROGRESS The applications have opened 350 of 2,000 pods\n"))
                .isEqualTo(WorkloadStartup.Line.PROGRESS);
        // The same count again isn't progress, so that a stuck startup times out
        assertThat(startup.accept("PROGRESS The applications have opened 350 of 2,000 pods\n"))
                .isEqualTo(WorkloadStartup.Line.OTHER);
        assertThat(startup.accept("PROGRESS The applications have opened 700 of 2,000 pods\n"
                + "READY applications=20 clients=2000\n")).isEqualTo(WorkloadStartup.Line.READY);

        assertThat(shown).containsExactly("The applications have opened 350 of 2,000 pods",
                "The applications have opened 700 of 2,000 pods");
        assertThat(startup.output()).startsWith("SLF4J(W): No SLF4J providers were found.\n")
                .endsWith("READY applications=20 clients=2000\n");
        assertThat(startup.failure()).isNull();
    }

    @Test
    public void failsWhenOutputKeepsComingWithoutProgress() {
        WorkloadStartup startup = new WorkloadStartup(".*READY applications=.*", message -> { });
        // Each poll takes a second, and returns a warning of a retry rather than progress
        AtomicLong nanos = new AtomicLong();
        WorkloadStartup.Frames retries = () -> {
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(1));
            return "WARN Connection refused, retrying\n";
        };

        assertThatThrownBy(() -> startup.awaitReady(retries, () -> true, nanos::get))
                .isInstanceOf(ContainerLaunchException.class)
                .hasMessageContaining("made no progress for 60 s");
        assertThat(startup.failure()).startsWith("made no progress for 60 s");
        assertThat(nanos.get()).isLessThanOrEqualTo(TimeUnit.SECONDS.toNanos(62));
    }
}
