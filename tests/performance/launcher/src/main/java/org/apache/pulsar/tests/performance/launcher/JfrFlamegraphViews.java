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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import one.convert.Main;

/**
 * Renders the async-profiler flame graph views of a recording: CPU, wall clock, allocation and lock, each only when
 * the profiler options asked for that event.
 *
 * <p>The views are rendered from the measurement recording when there is one, into a sibling
 * {@code <recording>-flamegraphs/} directory holding {@code <view>.html}, {@code <view>-threads.html} (split by
 * thread) and {@code <view>.collapsed} for scripts and diff tools. A view is decided by the options rather than by
 * the events in the file: {@code jfrsync} copies JDK events such as monitor waits into the recording, and a lock
 * view built from those would answer a question the scenario did not ask.
 */
final class JfrFlamegraphViews {
    static final String OUTPUT_SUFFIX = "-flamegraphs";

    enum View {
        CPU(false),
        WALL(false),
        ALLOC(true),
        LOCK(true);

        // Allocation and lock samples carry a size or a duration, which is what their graphs should weigh
        private final boolean total;

        View(boolean total) {
            this.total = total;
        }

        String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private JfrFlamegraphViews() {
    }

    /**
     * The views that the async-profiler options enable. {@code event=cpu} (or {@code itimer}, {@code ctimer},
     * {@code cpu-clock}) enables CPU, {@code event=wall} or {@code wall} wall clock, {@code alloc} allocation and
     * {@code lock} lock, with or without a value. Without an {@code event} and without any of the other three,
     * async-profiler samples CPU.
     */
    static Set<View> configuredViews(String asyncProfilerOptions) {
        Set<View> views = EnumSet.noneOf(View.class);
        if (asyncProfilerOptions == null || asyncProfilerOptions.isBlank()) {
            return views;
        }
        boolean eventGiven = false;
        for (String option : asyncProfilerOptions.split(",")) {
            int separator = option.indexOf('=');
            String key = (separator < 0 ? option : option.substring(0, separator)).trim();
            String value = separator < 0 ? "" : option.substring(separator + 1).trim();
            switch (key) {
                case "event" -> {
                    eventGiven = true;
                    switch (value) {
                        case "cpu", "itimer", "ctimer", "cpu-clock" -> views.add(View.CPU);
                        case "wall" -> views.add(View.WALL);
                        case "alloc" -> views.add(View.ALLOC);
                        case "lock" -> views.add(View.LOCK);
                        default -> {
                            // Hardware counters and other events have no view here
                        }
                    }
                }
                case "wall" -> views.add(View.WALL);
                case "alloc" -> views.add(View.ALLOC);
                case "lock" -> views.add(View.LOCK);
                default -> {
                    // Intervals, output and jfrsync options do not enable a view
                }
            }
        }
        if (!eventGiven && views.isEmpty()) {
            views.add(View.CPU);
        }
        return views;
    }

    /**
     * Renders the given views of {@code source} next to {@code recording}.
     *
     * @param recording the original recording, which names the output directory
     * @param source the recording to render, usually the measurement recording
     * @return the output directory
     */
    static Path render(Path recording, Path source, Set<View> views) throws IOException {
        String name = recording.getFileName().toString();
        Path directory = recording.resolveSibling(name.substring(0, name.length() - ".jfr".length())
                + OUTPUT_SUFFIX);
        Files.createDirectories(directory);
        for (View view : views) {
            Path collapsed = directory.resolve(view.label() + ".collapsed");
            convert(view, source, collapsed, "-o", "collapsed");
            if (Files.size(collapsed) == 0) {
                // The option was set but nothing was sampled, e.g. no lock was contended long enough
                Files.delete(collapsed);
                System.out.println("No " + view.label() + " samples in " + source);
                continue;
            }
            String title = view.label() + " " + name;
            convert(view, source, directory.resolve(view.label() + ".html"), "--title", title);
            convert(view, source, directory.resolve(view.label() + "-threads.html"), "--threads", "--title",
                    title + " by thread");
        }
        return directory;
    }

    private static void convert(View view, Path source, Path output, String... options) throws IOException {
        List<String> arguments = new ArrayList<>();
        arguments.add("--" + view.label());
        if (view.total) {
            arguments.add("--total");
        }
        arguments.addAll(List.of(options));
        arguments.add(source.toString());
        arguments.add(output.toString());
        try {
            Main.main(arguments.toArray(String[]::new));
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Rendering " + output + " failed", e);
        }
    }
}
