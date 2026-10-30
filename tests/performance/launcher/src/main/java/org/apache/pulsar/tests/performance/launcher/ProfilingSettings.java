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
 *     jfrEventConfig:
 *       - event: jdk.CPULoad
 *         setting: period
 *         value: 100 ms
 *     nettyAllocationsReport: true
 * </pre>
 *
 * <p>A component with {@code asyncProfilerOptions}, a comma-separated list of the options of
 * <a href="https://github.com/async-profiler/async-profiler/blob/master/docs/ProfilerOptions.md">async-profiler</a> as
 * its "Launch as agent" column names them, is profiled; {@code offCpuOptions} is the jonoffcpu agent's
 * {@code sampling} block for it, which a profiled component needs. When the component lists JFR configurations in
 * {@code jfrConfigurations}, {@link #DEFAULT_JFR_CONFIGURATIONS} unless the scenario lists others, or JFR events in
 * {@code jfrEventConfig}, the launcher adds async-profiler's {@code jfrsync} option, which records JDK Flight
 * Recorder's events alongside async-profiler's with their merge: a name, such as {@code profile}, is one of the JDK's
 * configurations, a name ending with {@code .jfc} is a file of {@value #JFR_CONFIGURATIONS_DIRECTORY}, and the events
 * and event settings of {@code jfrEventConfig} apply after them. Without {@code jfrEventConfig},
 * {@code jfrConfigurations} of {@code []} or {@code [none]} leave {@code jfrsync} out, and the component records only
 * async-profiler's events. {@code nettyAllocationsReport} summarizes the recording's Netty allocator events after the
 * run, which a component records when its {@code jfrConfigurations} list {@code netty-allocations.jfc}.
 */
record ProfilingSettings(Component broker, Component gateways, Component applications) {
    static final String BROKER = "broker";
    static final String GATEWAYS = "gateways";
    static final String APPLICATIONS = "applications";
    private static final List<String> COMPONENTS = List.of(BROKER, GATEWAYS, APPLICATIONS);
    private static final String ASYNC_PROFILER_OPTIONS = "asyncProfilerOptions";
    private static final String OFF_CPU_OPTIONS = "offCpuOptions";
    private static final String JFR_CONFIGURATIONS = "jfrConfigurations";
    private static final String JFR_EVENT_CONFIG = "jfrEventConfig";
    private static final String NETTY_ALLOCATIONS_REPORT = "nettyAllocationsReport";
    private static final Set<String> COMPONENT_KEYS = Set.of(ASYNC_PROFILER_OPTIONS, OFF_CPU_OPTIONS,
            JFR_CONFIGURATIONS, JFR_EVENT_CONFIG, NETTY_ALLOCATIONS_REPORT);
    private static final String SETTINGS = ASYNC_PROFILER_OPTIONS + ", " + OFF_CPU_OPTIONS + ", " + JFR_CONFIGURATIONS
            + ", " + JFR_EVENT_CONFIG + " and " + NETTY_ALLOCATIONS_REPORT;
    // A configuration of the JDK, such as profile, or the name of a .jfc file, without a directory
    private static final Pattern JFR_CONFIGURATION_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");
    // An event's name, such as jdk.CPULoad or io.netty.AllocateBuffer, and a setting's name, such as period
    private static final Pattern JFR_EVENT_NAME = Pattern.compile("[A-Za-z0-9_$]+(\\.[A-Za-z0-9_$]+)*");
    private static final Pattern JFR_SETTING_NAME = Pattern.compile("[A-Za-z0-9_]+");

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

    /** The configuration that {@code jfr configure} starts from without configurations: the JDK's own default one. */
    static final String JFR_CONFIGURE_DEFAULT_INPUT = "default";

    /** The configuration that {@code jfr configure} takes for an empty one, to start event settings from. */
    static final String JFR_CONFIGURE_EMPTY_INPUT = "none";

    /** The JDK's own profiling configuration, for an image whose JDK can't merge the configurations. */
    static final String FALLBACK_JFR_CONFIGURATION = "profile";

    /**
     * A component's settings.
     *
     * @param asyncProfilerOptions the async-profiler options, or null when the component isn't profiled
     * @param offCpuOptions the jonoffcpu agent's {@code sampling} block, as the scenario wrote it
     * @param jfrConfigurations the JFR configurations to merge and record with, in order
     * @param jfrEventConfig JFR events and event settings to add to the configurations, in order
     * @param nettyAllocationsReport whether to summarize the recording's Netty allocator events after the run
     */
    record Component(String asyncProfilerOptions, Map<String, Object> offCpuOptions, List<String> jfrConfigurations,
                     List<JfrEventSetting> jfrEventConfig, boolean nettyAllocationsReport) {
        static final Component NONE = new Component(null, Map.of(), DEFAULT_JFR_CONFIGURATIONS, List.of(), false);

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
                    jfrConfigurations, jfrEventConfig, nettyAllocationsReport);
        }

        /**
         * Whether the component records JFR's events, which {@code jfrConfigurations} of none, {@code []} or
         * {@code [none]}, without {@code jfrEventConfig} turn off.
         */
        boolean recordsJfrEvents() {
            return !jfrEventConfig.isEmpty() || !(jfrConfigurations.isEmpty()
                    || jfrConfigurations.equals(List.of(JFR_CONFIGURE_EMPTY_INPUT)));
        }

        /**
         * The {@code jfrEventConfig} as {@code jfr configure} takes it, which apply after the configurations:
         * {@code +event#setting=value}, and {@code +event#enabled=true} for an event without settings.
         */
        List<String> jfrConfigureEventSettings() {
            return jfrEventConfig.stream().map(event -> "+" + event.event() + "#"
                    + (event.setting() != null ? event.setting() + "=" + event.value() : "enabled=true")).toList();
        }

        /**
         * The {@code --input} of {@code jfr configure} that merges the configurations: a configuration of the JDK as it
         * is, and a {@code .jfc} file in {@code directory}, the directory {@value #JFR_CONFIGURATIONS_DIRECTORY} as the
         * merging container sees it. Without configurations, it is the JDK's {@code default} configuration, as
         * {@code jfr configure} starts from without {@code --input}; {@code none} starts from an empty configuration.
         */
        String jfrConfigureInput(String directory) {
            if (jfrConfigurations.isEmpty()) {
                return JFR_CONFIGURE_DEFAULT_INPUT;
            }
            List<String> inputs = new ArrayList<>();
            for (String configuration : jfrConfigurations) {
                inputs.add(configuration.endsWith(JFC_SUFFIX) ? directory + "/" + configuration : configuration);
            }
            return String.join(",", inputs);
        }
    }

    /**
     * An entry of {@code jfrEventConfig}: an event to enable, or with {@code setting} and {@code value}, a setting
     * of an event, such as {@code event: jdk.CPULoad, setting: period, value: 100 ms}.
     *
     * @param setting the setting's name, or null to enable the event
     * @param value the setting's value, or null without a setting
     */
    record JfrEventSetting(String event, String setting, String value) {
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
                    + " and add JFR events with " + JFR_EVENT_CONFIG);
        }
        List<String> jfrConfigurations = jfrConfigurations(section.path(JFR_CONFIGURATIONS), name);
        List<JfrEventSetting> jfrEventConfig = jfrEventConfig(section.path(JFR_EVENT_CONFIG), name);
        JsonNode report = section.path(NETTY_ALLOCATIONS_REPORT);
        if (!report.isMissingNode() && !report.isNull() && !report.isBoolean()) {
            throw new IllegalArgumentException("profiling." + name + "." + NETTY_ALLOCATIONS_REPORT
                    + " must be true or false");
        }
        boolean nettyAllocationsReport = report.asBoolean(false);
        JsonNode offCpu = section.path(OFF_CPU_OPTIONS);
        if (offCpu.isMissingNode() || offCpu.isNull()) {
            return new Component(asyncProfilerOptions, Map.of(), jfrConfigurations, jfrEventConfig,
                    nettyAllocationsReport);
        }
        if (!offCpu.isObject()) {
            throw new IllegalArgumentException("profiling." + name + "." + OFF_CPU_OPTIONS
                    + " must be the jonoffcpu agent's sampling block");
        }
        // Types are kept as the scenario wrote them, so that a quoted probability such as "0.010" stays a string
        // and is recorded in the capture metadata as spelled
        return new Component(asyncProfilerOptions,
                mapper.convertValue(offCpu, new TypeReference<LinkedHashMap<String, Object>>() { }),
                jfrConfigurations, jfrEventConfig, nettyAllocationsReport);
    }

    private static List<String> jfrConfigurations(JsonNode node, String name) {
        if (node.isMissingNode() || node.isNull()) {
            return DEFAULT_JFR_CONFIGURATIONS;
        }
        String invalid = "profiling." + name + "." + JFR_CONFIGURATIONS + " must list JFR configurations to merge:"
                + " configurations of the JDK, such as profile, .jfc files of " + JFR_CONFIGURATIONS_DIRECTORY
                + ", such as netty-allocations.jfc, or none";
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

    private static List<JfrEventSetting> jfrEventConfig(JsonNode node, String name) {
        if (node.isMissingNode() || node.isNull()) {
            return List.of();
        }
        String invalid = "profiling." + name + "." + JFR_EVENT_CONFIG + " must list JFR events to add, each with"
                + " event, and optionally setting and value, such as {event: jdk.CPULoad, setting: period,"
                + " value: 100 ms}";
        if (!node.isArray()) {
            throw new IllegalArgumentException(invalid + ", not " + node);
        }
        List<JfrEventSetting> events = new ArrayList<>();
        for (JsonNode entry : node) {
            if (!entry.isObject()) {
                throw new IllegalArgumentException(invalid + ", not " + entry);
            }
            entry.fieldNames().forEachRemaining(field -> {
                if (!List.of("event", "setting", "value").contains(field)) {
                    throw new IllegalArgumentException(invalid + ", not " + entry);
                }
            });
            JsonNode event = entry.path("event");
            JsonNode setting = entry.path("setting");
            JsonNode value = entry.path("value");
            boolean hasSetting = !setting.isMissingNode() && !setting.isNull();
            boolean hasValue = !value.isMissingNode() && !value.isNull();
            if (!event.isTextual() || !JFR_EVENT_NAME.matcher(event.textValue()).matches() || hasSetting != hasValue
                    || hasSetting && (!setting.isTextual() || !JFR_SETTING_NAME.matcher(setting.textValue()).matches()
                    || !value.isValueNode() || value.asText().isEmpty())) {
                throw new IllegalArgumentException(invalid + ", not " + entry);
            }
            events.add(new JfrEventSetting(event.textValue(), hasSetting ? setting.textValue() : null,
                    hasSetting ? value.asText() : null));
        }
        return List.copyOf(events);
    }
}
