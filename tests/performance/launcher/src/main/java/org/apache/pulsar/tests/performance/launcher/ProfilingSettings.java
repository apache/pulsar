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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A scenario's {@code profiling} section: the profiler settings of each component, the broker, the gateways (the
 * producer) and the applications (the consumers).
 *
 * <pre>
 * profiling:
 *   broker:
 *     asyncProfilerOptions: event=cpu,interval=10ms,alloc=2m,jfrsync=profile
 *     offCpuOptions:
 *       reasons: [blocked]
 *       ...
 * </pre>
 *
 * <p>A component with {@code asyncProfilerOptions} is profiled; {@code offCpuOptions} is the jonoffcpu agent's
 * {@code sampling} block for it, which a profiled component needs.
 */
record ProfilingSettings(Component broker, Component gateways, Component applications) {
    static final String BROKER = "broker";
    static final String GATEWAYS = "gateways";
    static final String APPLICATIONS = "applications";
    private static final List<String> COMPONENTS = List.of(BROKER, GATEWAYS, APPLICATIONS);
    private static final String ASYNC_PROFILER_OPTIONS = "asyncProfilerOptions";
    private static final String OFF_CPU_OPTIONS = "offCpuOptions";
    private static final Set<String> COMPONENT_KEYS = Set.of(ASYNC_PROFILER_OPTIONS, OFF_CPU_OPTIONS);

    /**
     * A component's settings.
     *
     * @param asyncProfilerOptions the async-profiler options, or null when the component isn't profiled
     * @param offCpuOptions the jonoffcpu agent's {@code sampling} block, as the scenario wrote it
     */
    record Component(String asyncProfilerOptions, Map<String, Object> offCpuOptions) {
        static final Component NONE = new Component(null, Map.of());

        boolean profiled() {
            return asyncProfilerOptions != null;
        }
    }

    /** Reads the {@code profiling} section, which may be missing. */
    static ProfilingSettings read(ObjectMapper mapper, JsonNode profiling) {
        if (profiling.isMissingNode() || profiling.isNull()) {
            return new ProfilingSettings(Component.NONE, Component.NONE, Component.NONE);
        }
        if (!profiling.isObject()) {
            throw new IllegalArgumentException("profiling must be a mapping of " + COMPONENTS);
        }
        profiling.fieldNames().forEachRemaining(field -> {
            if (!COMPONENTS.contains(field)) {
                throw new IllegalArgumentException("profiling." + field + " isn't a component; profiling has "
                        + COMPONENTS + ", each with " + ASYNC_PROFILER_OPTIONS + " and " + OFF_CPU_OPTIONS);
            }
        });
        return new ProfilingSettings(component(mapper, profiling, BROKER), component(mapper, profiling, GATEWAYS),
                component(mapper, profiling, APPLICATIONS));
    }

    boolean anyProfiled() {
        return broker.profiled() || gateways.profiled() || applications.profiled();
    }

    private static Component component(ObjectMapper mapper, JsonNode profiling, String name) {
        JsonNode section = profiling.path(name);
        if (section.isMissingNode() || section.isNull()) {
            return Component.NONE;
        }
        if (!section.isObject()) {
            throw new IllegalArgumentException("profiling." + name + " must be a mapping with "
                    + ASYNC_PROFILER_OPTIONS + " and " + OFF_CPU_OPTIONS);
        }
        section.fieldNames().forEachRemaining(field -> {
            if (!COMPONENT_KEYS.contains(field)) {
                throw new IllegalArgumentException("profiling." + name + "." + field + " isn't a setting; a component "
                        + "has " + ASYNC_PROFILER_OPTIONS + " and " + OFF_CPU_OPTIONS);
            }
        });
        JsonNode options = section.path(ASYNC_PROFILER_OPTIONS);
        String asyncProfilerOptions = options.isTextual() && !options.textValue().isBlank()
                ? options.textValue() : null;
        JsonNode offCpu = section.path(OFF_CPU_OPTIONS);
        if (offCpu.isMissingNode() || offCpu.isNull()) {
            return new Component(asyncProfilerOptions, Map.of());
        }
        if (!offCpu.isObject()) {
            throw new IllegalArgumentException("profiling." + name + "." + OFF_CPU_OPTIONS
                    + " must be the jonoffcpu agent's sampling block");
        }
        // Types are kept as the scenario wrote them, so that a quoted probability such as "0.010" stays a string
        // and is recorded in the capture metadata as spelled
        return new Component(asyncProfilerOptions,
                mapper.convertValue(offCpu, new TypeReference<LinkedHashMap<String, Object>>() { }));
    }
}
