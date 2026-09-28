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
}
