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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class HeapDumpSettingsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void readsEachComponentsSettings() throws Exception {
        HeapDumpSettings settings = HeapDumpSettings.read(mapper.readTree("""
                broker:
                  onOutOfMemoryError: true
                  atStart: true
                  atSeconds: [30, 90]
                  everySeconds: 60
                  atPeakUsage: true
                  atEnd: true
                applications:
                  atSeconds: 20
                """));

        assertThat(settings.any()).isTrue();
        assertThat(settings.broker()).isEqualTo(new HeapDumpSettings.Component(true, true, List.of(30, 90), 60, true,
                true));
        assertThat(settings.broker().scheduled()).isTrue();
        // A single time needn't be a list
        assertThat(settings.applications().atSeconds()).containsExactly(20);
        assertThat(settings.gateways()).isEqualTo(HeapDumpSettings.Component.NONE);
        assertThat(settings.gateways().any()).isFalse();
        // Uncompressed unless the section asks for a gzip level
        assertThat(settings.gzipLevel()).isZero();
    }

    @Test
    public void readsTheGzipLevelBesideTheComponents() throws Exception {
        HeapDumpSettings settings = HeapDumpSettings.read(mapper.readTree("""
                gzipLevel: 1
                broker:
                  atEnd: true
                """));

        assertThat(settings.gzipLevel()).isEqualTo(1);
        assertThat(settings.broker().atEnd()).isTrue();
        assertThatThrownBy(() -> HeapDumpSettings.read(mapper.readTree("gzipLevel: 10")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heapDumps.gzipLevel must be a whole number from 0, uncompressed, to 9");
        assertThatThrownBy(() -> HeapDumpSettings.read(mapper.readTree("gzipLevel: true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heapDumps.gzipLevel must be a whole number");
    }

    @Test
    public void dumpsOnlyOnOutOfMemoryErrorWithoutScheduledDumps() throws Exception {
        HeapDumpSettings.Component gateways = HeapDumpSettings.read(mapper.readTree("""
                gateways:
                  onOutOfMemoryError: true
                """)).gateways();

        assertThat(gateways.any()).isTrue();
        assertThat(gateways.scheduled()).isFalse();
    }

    @Test
    public void dumpsNothingWithoutAHeapDumpsSection() throws Exception {
        assertThat(HeapDumpSettings.read(mapper.readTree("workloads: {}").path("heapDumps")).any()).isFalse();
    }

    @Test
    public void rejectsWhatItCannotDo() throws Exception {
        assertThatThrownBy(() -> HeapDumpSettings.read(mapper.readTree("bookies:\n  atStart: true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heapDumps.bookies isn't a component or a setting");
        assertThatThrownBy(() -> HeapDumpSettings.read(mapper.readTree("broker:\n  atPeak: true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heapDumps.broker.atPeak isn't a setting");
        // The gateways and the applications have exited when every message has been received
        assertThatThrownBy(() -> HeapDumpSettings.read(mapper.readTree("gateways:\n  atEnd: true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heapDumps.gateways.atEnd is only for the broker");
        assertThatThrownBy(() -> HeapDumpSettings.read(mapper.readTree("broker:\n  everySeconds: -1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heapDumps.broker.everySeconds must be a whole number of seconds");
        assertThatThrownBy(() -> HeapDumpSettings.read(mapper.readTree("broker:\n  atSeconds: [1.5]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heapDumps.broker.atSeconds must be a whole number of seconds");
        assertThatThrownBy(() -> HeapDumpSettings.read(mapper.readTree("broker:\n  atStart: yes please")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heapDumps.broker.atStart must be true or false");
    }

    @Test
    public void addsTheOutOfMemoryOptionsAfterTheConfiguredOptions() {
        assertThat(PerformanceLauncher.withJvmOptions(Map.of("PULSAR_MEM", "-Xmx1g"), "PULSAR_EXTRA_OPTS",
                HeapDumper.outOfMemoryOptions(0)))
                .containsEntry("PULSAR_MEM", "-Xmx1g")
                .containsEntry("PULSAR_EXTRA_OPTS", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/heap-dumps");
        // The JVM names a compressed dump java_pid<pid>.hprof.gz itself
        assertThat(HeapDumper.outOfMemoryOptions(1)).isEqualTo(
                "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/heap-dumps -XX:HeapDumpGzipLevel=1");
        assertThat(PerformanceLauncher.withJvmOptions(Map.of("PULSAR_EXTRA_OPTS", "-Dx=1"), "PULSAR_EXTRA_OPTS",
                "-Dy=2")).containsEntry("PULSAR_EXTRA_OPTS", "-Dx=1 -Dy=2");
        assertThat(PerformanceLauncher.withJvmOptions(null, "PULSAR_EXTRA_OPTS", "-Dy=2"))
                .containsEntry("PULSAR_EXTRA_OPTS", "-Dy=2");
    }
}
