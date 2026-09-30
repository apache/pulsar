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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A scenario's {@code profiling} section: the profiler settings of each component, the broker, the gateways (the
 * producer) and the applications (the consumers).
 *
 * <pre>
 * profiling:
 *   broker:
 *     asyncProfilerOptions: event=cpu,interval=10ms,alloc=2m
 *     offCpuOptions:
 *       reasons: [blocked]
 *       ...
 *     jfrConfigurations: [profile, netty-allocations.jfc]
 *     nettyAllocationsReport: true
 * </pre>
 *
 * <p>A component with {@code asyncProfilerOptions}, a comma-separated list of the options of
 * <a href="https://github.com/async-profiler/async-profiler/blob/master/docs/ProfilerOptions.md">async-profiler</a>,
 * as the page's "Launch as agent" column names them, is profiled; {@code offCpuOptions} is the jonoffcpu agent's
 * {@code sampling} block for it, which a profiled component needs. When the component lists JFR configurations in
 * {@code jfrConfigurations}, {@link #DEFAULT_JFR_CONFIGURATIONS} unless the scenario lists others, the launcher adds
 * async-profiler's {@code jfrsync} option, which records JDK Flight Recorder's events alongside async-profiler's with
 * their merge: a name, such as {@code profile}, is one of the JDK's configurations, and a name ending with
 * {@code .jfc} is a file of {@value #JFR_CONFIGURATIONS_DIRECTORY}. An empty list leaves {@code jfrsync} out, which
 * records only async-profiler's events. {@code nettyAllocationsReport} summarizes the recording's Netty allocator events after
 * the run, which a component records when its {@code jfrConfigurations} list {@code netty-allocations.jfc}.
 */
record ProfilingSettings(Component broker, Component gateways, Component applications) {
    static final String BROKER = "broker";
    static final String GATEWAYS = "gateways";
    static final String APPLICATIONS = "applications";
    private static final List<String> COMPONENTS = List.of(BROKER, GATEWAYS, APPLICATIONS);
    private static final String ASYNC_PROFILER_OPTIONS = "asyncProfilerOptions";
    private static final String OFF_CPU_OPTIONS = "offCpuOptions";
    private static final String JFR_CONFIGURATIONS = "jfrConfigurations";
    private static final String NETTY_ALLOCATIONS_REPORT = "nettyAllocationsReport";
    private static final Set<String> COMPONENT_KEYS = Set.of(ASYNC_PROFILER_OPTIONS, OFF_CPU_OPTIONS,
            JFR_CONFIGURATIONS, NETTY_ALLOCATIONS_REPORT);
    private static final String SETTINGS = ASYNC_PROFILER_OPTIONS + ", " + OFF_CPU_OPTIONS + ", " + JFR_CONFIGURATIONS
            + " and " + NETTY_ALLOCATIONS_REPORT;
    // A configuration of the JDK, such as profile, or the name of a .jfc file, without a directory
    private static final Pattern JFR_CONFIGURATION_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");

    /** async-profiler's option that records JDK Flight Recorder's events alongside its own. */
    static final String JFRSYNC = "jfrsync";

    /** The directory, relative to the repository, of the JFR configuration files that a component can list. */
    static final String JFR_CONFIGURATIONS_DIRECTORY = "tests/performance/jfr";

    static final String JFC_SUFFIX = ".jfc";

    /**
     * The JFR configurations that a profiled component records with unless the scenario lists them: the JDK's
     * {@code profile} configuration, which the JDK describes as a profiling configuration with about 2 % overhead.
     */
    static final List<String> DEFAULT_JFR_CONFIGURATIONS = List.of("profile");

    /** The JDK's own profiling configuration, for an image whose JDK can't merge the configurations. */
    static final String FALLBACK_JFR_CONFIGURATION = "profile";

    /**
     * A component's settings.
     *
     * @param asyncProfilerOptions the async-profiler options, or null when the component isn't profiled
     * @param offCpuOptions the jonoffcpu agent's {@code sampling} block, as the scenario wrote it
     * @param jfrConfigurations the JFR configurations to merge and record with, in order; none records without JFR's
     *                          events
     * @param nettyAllocationsReport whether to summarize the recording's Netty allocator events after the run
     */
    record Component(String asyncProfilerOptions, Map<String, Object> offCpuOptions, List<String> jfrConfigurations,
                     boolean nettyAllocationsReport) {
        static final Component NONE = new Component(null, Map.of(), DEFAULT_JFR_CONFIGURATIONS, false);

        boolean profiled() {
            return asyncProfilerOptions != null;
        }

        /**
         * The component with async-profiler's {@code jfrsync} recording JFR's events with {@code configuration}, or
         * without JFR's events when it is null.
         */
        Component withJfrsync(String configuration) {
            if (!profiled() || configuration == null) {
                return this;
            }
            return new Component(asyncProfilerOptions + "," + JFRSYNC + "=" + configuration, offCpuOptions,
                    jfrConfigurations, nettyAllocationsReport);
        }

        /** Whether the component records JFR's events, which an empty {@code jfrConfigurations} turns off. */
        boolean recordsJfrEvents() {
            return !jfrConfigurations.isEmpty();
        }

        /**
         * The {@code --input} of {@code jfr configure} that merges the configurations: a configuration of the JDK as it
         * is, and a {@code .jfc} file in {@code directory}, the directory {@value #JFR_CONFIGURATIONS_DIRECTORY} as the
         * merging container sees it.
         */
        String jfrConfigureInput(String directory) {
            List<String> inputs = new ArrayList<>();
            for (String configuration : jfrConfigurations) {
                inputs.add(configuration.endsWith(JFC_SUFFIX) ? directory + "/" + configuration : configuration);
            }
            return String.join(",", inputs);
        }
    }

    /** The settings with each profiled component's {@code jfrsync} set to its configuration, by component name. */
    ProfilingSettings withJfrsync(Map<String, String> configurations) {
        return new ProfilingSettings(broker.withJfrsync(configurations.get(BROKER)),
                gateways.withJfrsync(configurations.get(GATEWAYS)),
                applications.withJfrsync(configurations.get(APPLICATIONS)));
    }

    /** The component's settings by its name. */
    Component component(String name) {
        return switch (name) {
            case BROKER -> broker;
            case GATEWAYS -> gateways;
            case APPLICATIONS -> applications;
            default -> throw new IllegalArgumentException("Not a component: " + name);
        };
    }

    static List<String> components() {
        return COMPONENTS;
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
                        + COMPONENTS + ", each with " + SETTINGS);
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
            throw new IllegalArgumentException("profiling." + name + " must be a mapping with " + SETTINGS);
        }
        section.fieldNames().forEachRemaining(field -> {
            if (!COMPONENT_KEYS.contains(field)) {
                throw new IllegalArgumentException("profiling." + name + "." + field + " isn't a setting; a component "
                        + "has " + SETTINGS);
            }
        });
        JsonNode options = section.path(ASYNC_PROFILER_OPTIONS);
        String asyncProfilerOptions = options.isTextual() && !options.textValue().isBlank()
                ? options.textValue() : null;
        // The launcher sets jfrsync from the component's jfrConfigurations
        if (asyncProfilerOptions != null && Arrays.stream(asyncProfilerOptions.split(","))
                .anyMatch(option -> option.trim().startsWith(JFRSYNC))) {
            throw new IllegalArgumentException("profiling." + name + "." + ASYNC_PROFILER_OPTIONS + " sets " + JFRSYNC
                    + ", which the launcher sets to the merge of the component's " + JFR_CONFIGURATIONS + "; list the"
                    + " JFR configurations there, such as " + JFR_CONFIGURATIONS + ": [profile, netty-allocations.jfc],"
                    + " or none, " + JFR_CONFIGURATIONS + ": [], to record without JFR's events");
        }
        List<String> jfrConfigurations = jfrConfigurations(section.path(JFR_CONFIGURATIONS), name);
        JsonNode report = section.path(NETTY_ALLOCATIONS_REPORT);
        if (!report.isMissingNode() && !report.isNull() && !report.isBoolean()) {
            throw new IllegalArgumentException("profiling." + name + "." + NETTY_ALLOCATIONS_REPORT
                    + " must be true or false");
        }
        boolean nettyAllocationsReport = report.asBoolean(false);
        JsonNode offCpu = section.path(OFF_CPU_OPTIONS);
        if (offCpu.isMissingNode() || offCpu.isNull()) {
            return new Component(asyncProfilerOptions, Map.of(), jfrConfigurations, nettyAllocationsReport);
        }
        if (!offCpu.isObject()) {
            throw new IllegalArgumentException("profiling." + name + "." + OFF_CPU_OPTIONS
                    + " must be the jonoffcpu agent's sampling block");
        }
        // Types are kept as the scenario wrote them, so that a quoted probability such as "0.010" stays a string
        // and is recorded in the capture metadata as spelled
        return new Component(asyncProfilerOptions,
                mapper.convertValue(offCpu, new TypeReference<LinkedHashMap<String, Object>>() { }),
                jfrConfigurations, nettyAllocationsReport);
    }

    private static List<String> jfrConfigurations(JsonNode node, String name) {
        if (node.isMissingNode() || node.isNull()) {
            return DEFAULT_JFR_CONFIGURATIONS;
        }
        String invalid = "profiling." + name + "." + JFR_CONFIGURATIONS + " must list JFR configurations to merge:"
                + " configurations of the JDK, such as profile, and .jfc files of " + JFR_CONFIGURATIONS_DIRECTORY
                + ", such as netty-allocations.jfc, or none to record without JFR's events";
        if (!node.isArray()) {
            throw new IllegalArgumentException(invalid + ", not " + node);
        }
        List<String> configurations = new ArrayList<>();
        for (JsonNode configuration : node) {
            if (!configuration.isTextual() || !JFR_CONFIGURATION_NAME.matcher(configuration.textValue()).matches()) {
                throw new IllegalArgumentException(invalid + ", not " + configuration);
            }
            configurations.add(configuration.textValue());
        }
        return List.copyOf(configurations);
    }
}
