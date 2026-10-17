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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ProfilingSettingsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void readsEachComponentsSettings() throws Exception {
        ProfilingSettings settings = ProfilingSettings.read(mapper, mapper.readTree("""
                broker:
                  asyncProfilerOptions: event=cpu,interval=10ms
                  offCpuOptions:
                    reasons: [blocked]
                    admission:
                      policy: none
                applications:
                  offCpuOptions:
                    minOffCpuMicros: 100
                """));

        assertThat(settings.anyProfiled()).isTrue();
        assertThat(settings.broker().profiled()).isTrue();
        assertThat(settings.broker().asyncProfilerOptions()).isEqualTo("event=cpu,interval=10ms");
        assertThat(settings.broker().offCpuOptions())
                .containsEntry("reasons", List.of("blocked"))
                .containsEntry("admission", Map.of("policy", "none"));
        // Off-CPU options alone don't profile a component
        assertThat(settings.applications().profiled()).isFalse();
        assertThat(settings.gateways()).isEqualTo(ProfilingSettings.Component.NONE);
    }

    @Test
    public void profilesNothingWithoutAProfilingSection() throws Exception {
        assertThat(ProfilingSettings.read(mapper, mapper.readTree("workloads: {}").path("profiling")).anyProfiled())
                .isFalse();
    }

    @Test
    public void rejectsTheSettingsOfTheFormerLayout() throws Exception {
        assertThatThrownBy(() -> ProfilingSettings.read(mapper, mapper.readTree("brokerOptions: event=cpu")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profiling.brokerOptions isn't a component");
        assertThatThrownBy(() -> ProfilingSettings.read(mapper,
                mapper.readTree("broker:\n  retainOriginalRecording: true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profiling.broker.retainOriginalRecording isn't a setting");
    }

    @Test
    public void addsTheJfrConfigurationToTheProfiledComponents() throws Exception {
        ProfilingSettings settings = ProfilingSettings.read(mapper, mapper.readTree("""
                broker:
                  asyncProfilerOptions: event=cpu,interval=10ms
                  offCpuOptions:
                    admission:
                      policy: none
                """)).withJfrsync(Map.of(ProfilingSettings.BROKER, "/profiles/jfr-configuration.jfc",
                ProfilingSettings.GATEWAYS, ProfilingSettings.FALLBACK_JFR_CONFIGURATION));

        assertThat(settings.broker().asyncProfilerOptions())
                .isEqualTo("event=cpu,interval=10ms,jfrsync=/profiles/jfr-configuration.jfc");
        // A component that isn't profiled gets no options
        assertThat(settings.gateways()).isEqualTo(ProfilingSettings.Component.NONE);
    }

    @Test
    public void rejectsAJfrConfigurationInTheScenario() throws Exception {
        assertThatThrownBy(() -> ProfilingSettings.read(mapper,
                mapper.readTree("broker:\n  asyncProfilerOptions: event=cpu,jfrsync=profile")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profiling.broker.asyncProfilerOptions sets jfrsync")
                .hasMessageContaining("jfrConfigurations");
    }

    @Test
    public void mergesTheListedJfrConfigurations() throws Exception {
        ProfilingSettings settings = ProfilingSettings.read(mapper, mapper.readTree("""
                broker:
                  asyncProfilerOptions: event=cpu
                  jfrConfigurations: [profile, netty-allocations.jfc]
                  nettyAllocationsReport: true
                gateways:
                  asyncProfilerOptions: event=cpu
                applications:
                  asyncProfilerOptions: event=cpu
                  jfrConfigurations: []
                """));

        assertThat(settings.broker().jfrConfigurations()).containsExactly("profile", "netty-allocations.jfc");
        // A configuration of the JDK as it is, a .jfc file in the directory
        assertThat(settings.broker().jfrConfigureInput("/jfr")).isEqualTo("profile,/jfr/netty-allocations.jfc");
        assertThat(settings.broker().nettyAllocationsReport()).isTrue();
        assertThat(settings.gateways().jfrConfigurations()).containsExactly("profile");
        assertThat(settings.gateways().nettyAllocationsReport()).isFalse();
        // No configurations record without JFR's events, so the launcher adds no jfrsync
        assertThat(settings.applications().recordsJfrEvents()).isFalse();
        assertThat(settings.applications().withJfrsync(null).asyncProfilerOptions()).isEqualTo("event=cpu");
    }

    @Test
    public void rejectsInvalidJfrSettings() throws Exception {
        assertThatThrownBy(() -> ProfilingSettings.read(mapper,
                mapper.readTree("broker:\n  jfrConfigurations: [profile, ../etc/passwd.jfc]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profiling.broker.jfrConfigurations must list JFR configurations");
        assertThatThrownBy(() -> ProfilingSettings.read(mapper,
                mapper.readTree("broker:\n  jfrConfigurations: netty-allocations.jfc")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profiling.broker.jfrConfigurations must list JFR configurations");
        assertThatThrownBy(() -> ProfilingSettings.read(mapper,
                mapper.readTree("broker:\n  nettyAllocationsReport: yes please")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profiling.broker.nettyAllocationsReport must be true or false");
    }

    @Test
    public void findsTheComponentOfARecording() {
        Path run = Path.of("/reports/run");

        assertThat(PerformanceLauncher.recordingComponent(run, run.resolve("broker-profile/a.jfr")))
                .isEqualTo(ProfilingSettings.BROKER);
        assertThat(PerformanceLauncher.recordingComponent(run, run.resolve("gateways/profile-gateways-1.jfr")))
                .isEqualTo(ProfilingSettings.GATEWAYS);
        assertThat(PerformanceLauncher.recordingComponent(run, run.resolve("applications/profile-applications-1.jfr")))
                .isEqualTo(ProfilingSettings.APPLICATIONS);
    }

    @Test
    public void addsTheListedJfrEvents() throws Exception {
        ProfilingSettings settings = ProfilingSettings.read(mapper, mapper.readTree("""
                broker:
                  asyncProfilerOptions: event=cpu
                  jfrEventConfig:
                    - event: jdk.CPULoad
                      setting: period
                      value: 100 ms
                    - event: io.netty.AllocateChunk
                gateways:
                  asyncProfilerOptions: event=cpu
                  jfrConfigurations: []
                  jfrEventConfig:
                    - event: jdk.ThreadPark
                      setting: threshold
                      value: 0 ms
                """));

        assertThat(settings.broker().jfrEventConfig()).containsExactly(
                new ProfilingSettings.JfrEventSetting("jdk.CPULoad", "period", "100 ms"),
                new ProfilingSettings.JfrEventSetting("io.netty.AllocateChunk", null, null));
        // jfr configure applies them after the configurations; an event without a setting is enabled
        assertThat(settings.broker().jfrConfigureEventSettings())
                .containsExactly("+jdk.CPULoad#period=100 ms", "+io.netty.AllocateChunk#enabled=true");
        assertThat(PerformanceLauncher.jfrConfigureCommand("profile", settings.broker().jfrConfigureEventSettings(),
                "/out/jfr-configuration.jfc")).containsExactly("configure", "--input", "profile",
                "+jdk.CPULoad#period=100 ms", "+io.netty.AllocateChunk#enabled=true", "--output",
                "/out/jfr-configuration.jfc");
        // Without configurations, they are merged into the JDK's default configuration, as jfr configure does
        assertThat(settings.gateways().recordsJfrEvents()).isTrue();
        assertThat(settings.gateways().jfrConfigureInput("/jfr")).isEqualTo("default");
        assertThat(settings.gateways().jfrConfigureEventSettings()).containsExactly("+jdk.ThreadPark#threshold=0 ms");
        // none starts from an empty configuration instead
        assertThat(ProfilingSettings.read(mapper, mapper.readTree("""
                broker:
                  asyncProfilerOptions: event=cpu
                  jfrConfigurations: [none]
                  jfrEventConfig:
                    - event: jdk.ThreadPark
                """)).broker().jfrConfigureInput("/jfr")).isEqualTo("none");
        // Without event settings, none turns JFR off like no configurations
        assertThat(ProfilingSettings.read(mapper, mapper.readTree("""
                broker:
                  asyncProfilerOptions: event=cpu
                  jfrConfigurations: [none]
                """)).broker().recordsJfrEvents()).isFalse();
    }

    @Test
    public void rejectsInvalidJfrEvents() throws Exception {
        for (String events : List.of("jdk.CPULoad", "[{event: jdk.CPULoad, setting: period}]",
                "[{event: jdk.CPULoad, setting: period, value: ''}]", "[{event: 'jdk.CPU Load'}]",
                "[{event: jdk.CPULoad, other: x}]")) {
            assertThatThrownBy(() -> ProfilingSettings.read(mapper,
                    mapper.readTree("broker:\n  jfrEventConfig: " + events)))
                    .as(events)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("profiling.broker.jfrEventConfig must list JFR events to add");
        }
    }
}
