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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A scenario's {@code heapDumps} section: when to write a heap dump of each component's JVM, the broker, the gateways
 * (the producer) and the applications (the consumers).
 *
 * <pre>
 * heapDumps:
 *   gzipLevel: 1
 *   broker:
 *     onOutOfMemoryError: true
 *     atStart: true
 *     atSeconds: [60]
 *     everySeconds: 30
 *     atPeakUsage: true
 *     atEnd: true
 * </pre>
 *
 * <p>{@code gzipLevel}, from 1 to 9, has the JVMs write every dump gzip-compressed as {@code .hprof.gz}, at that level;
 * without it, or at 0, they write {@code .hprof}. The times count from the start of the gateways, which starts the
 * traffic. {@code atEnd} is after every
 * application has received every message, which only the broker outlives: the gateways and the applications have
 * exited by then.
 */
record HeapDumpSettings(int gzipLevel, Component broker, Component gateways, Component applications) {
    static final String BROKER = "broker";
    static final String GATEWAYS = "gateways";
    static final String APPLICATIONS = "applications";
    private static final List<String> COMPONENTS = List.of(BROKER, GATEWAYS, APPLICATIONS);
    static final String GZIP_LEVEL = "gzipLevel";
    // The levels of the JVM's gzip compression, of jcmd GC.heap_dump -gz and -XX:HeapDumpGzipLevel
    static final int MAX_GZIP_LEVEL = 9;
    static final String ON_OUT_OF_MEMORY_ERROR = "onOutOfMemoryError";
    static final String AT_START = "atStart";
    static final String AT_SECONDS = "atSeconds";
    static final String EVERY_SECONDS = "everySeconds";
    static final String AT_PEAK_USAGE = "atPeakUsage";
    static final String AT_END = "atEnd";
    private static final Set<String> COMPONENT_KEYS = Set.of(ON_OUT_OF_MEMORY_ERROR, AT_START, AT_SECONDS,
            EVERY_SECONDS, AT_PEAK_USAGE, AT_END);

    /**
     * A component's settings.
     *
     * @param onOutOfMemoryError whether the JVM writes a heap dump when it runs out of memory
     * @param atStart whether to dump when the gateways start
     * @param atSeconds the seconds after the gateways' start at which to dump
     * @param everySeconds the interval of the periodic dumps after the gateways' start, or 0 for none
     * @param atPeakUsage whether to keep a dump of the highest heap usage that the launcher samples
     * @param atEnd whether to dump after every application has received every message; the broker only
     */
    record Component(boolean onOutOfMemoryError, boolean atStart, List<Integer> atSeconds, int everySeconds,
                     boolean atPeakUsage, boolean atEnd) {
        static final Component NONE = new Component(false, false, List.of(), 0, false, false);

        /** Whether the launcher takes dumps of this component during the run, which excludes the JVM's own. */
        boolean scheduled() {
            return atStart || !atSeconds.isEmpty() || everySeconds > 0 || atPeakUsage || atEnd;
        }

        boolean any() {
            return onOutOfMemoryError || scheduled();
        }
    }

    /** Reads the {@code heapDumps} section, which may be missing. */
    static HeapDumpSettings read(JsonNode heapDumps) {
        if (heapDumps.isMissingNode() || heapDumps.isNull()) {
            return new HeapDumpSettings(0, Component.NONE, Component.NONE, Component.NONE);
        }
        if (!heapDumps.isObject()) {
            throw new IllegalArgumentException("heapDumps must be a mapping of " + COMPONENTS + " and " + GZIP_LEVEL);
        }
        heapDumps.fieldNames().forEachRemaining(field -> {
            if (!COMPONENTS.contains(field) && !GZIP_LEVEL.equals(field)) {
                throw new IllegalArgumentException("heapDumps." + field + " isn't a component or a setting; heapDumps"
                        + " has " + COMPONENTS + " and " + GZIP_LEVEL);
            }
        });
        JsonNode level = heapDumps.path(GZIP_LEVEL);
        int gzipLevel = 0;
        if (!level.isMissingNode() && !level.isNull()) {
            if (!level.isIntegralNumber() || !level.canConvertToInt() || level.intValue() < 0
                    || level.intValue() > MAX_GZIP_LEVEL) {
                throw new IllegalArgumentException("heapDumps." + GZIP_LEVEL + " must be a whole number from 0, "
                        + "uncompressed, to " + MAX_GZIP_LEVEL);
            }
            gzipLevel = level.intValue();
        }
        return new HeapDumpSettings(gzipLevel, component(heapDumps, BROKER), component(heapDumps, GATEWAYS),
                component(heapDumps, APPLICATIONS));
    }

    boolean any() {
        return broker.any() || gateways.any() || applications.any();
    }

    private static Component component(JsonNode heapDumps, String name) {
        JsonNode section = heapDumps.path(name);
        if (section.isMissingNode() || section.isNull()) {
            return Component.NONE;
        }
        String path = "heapDumps." + name;
        if (!section.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping with " + COMPONENT_KEYS);
        }
        section.fieldNames().forEachRemaining(field -> {
            if (!COMPONENT_KEYS.contains(field)) {
                throw new IllegalArgumentException(path + "." + field + " isn't a setting; a component has "
                        + List.of(ON_OUT_OF_MEMORY_ERROR, AT_START, AT_SECONDS, EVERY_SECONDS, AT_PEAK_USAGE,
                        AT_END));
            }
        });
        boolean atEnd = bool(section, AT_END, path);
        if (atEnd && !BROKER.equals(name)) {
            throw new IllegalArgumentException(path + "." + AT_END + " is only for the broker: the " + name
                    + " have exited when every application has received every message");
        }
        int everySeconds = seconds(section.path(EVERY_SECONDS), path + "." + EVERY_SECONDS, 0);
        List<Integer> atSeconds = new ArrayList<>();
        JsonNode times = section.path(AT_SECONDS);
        if (!times.isMissingNode() && !times.isNull()) {
            if (times.isNumber()) {
                atSeconds.add(seconds(times, path + "." + AT_SECONDS, 0));
            } else if (times.isArray()) {
                for (JsonNode time : times) {
                    atSeconds.add(seconds(time, path + "." + AT_SECONDS, 0));
                }
            } else {
                throw new IllegalArgumentException(path + "." + AT_SECONDS + " must be a number of seconds or a list"
                        + " of them");
            }
        }
        return new Component(bool(section, ON_OUT_OF_MEMORY_ERROR, path), bool(section, AT_START, path),
                List.copyOf(atSeconds), everySeconds, bool(section, AT_PEAK_USAGE, path), atEnd);
    }

    private static boolean bool(JsonNode section, String field, String path) {
        JsonNode value = section.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return false;
        }
        if (!value.isBoolean()) {
            throw new IllegalArgumentException(path + "." + field + " must be true or false");
        }
        return value.booleanValue();
    }

    private static int seconds(JsonNode value, String path, int defaultValue) {
        if (value.isMissingNode() || value.isNull()) {
            return defaultValue;
        }
        if (!value.canConvertToInt() || !value.isIntegralNumber() || value.intValue() < 0) {
            throw new IllegalArgumentException(path + " must be a whole number of seconds, 0 or more");
        }
        return value.intValue();
    }
}
