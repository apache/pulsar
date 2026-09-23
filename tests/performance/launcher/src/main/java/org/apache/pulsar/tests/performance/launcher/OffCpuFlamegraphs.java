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

import io.github.lhotari.jonoffcpu.offline.OffCpuCorrelator;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import one.convert.Main;
import org.apache.pulsar.tests.integration.profiling.JonoffcpuAgent;

/**
 * Turns a jonoffcpu capture into off-CPU flame graphs.
 *
 * <p>For a recording {@code profile.jfr} with its {@code profile.jonoffcpu-capture.pb} stream, the correlator
 * writes {@code profile-offcpu/} holding {@code jonoffcpu-offcpu-stacks.collapsed} (Java stacks weighted in
 * microseconds of off-CPU time), a synthetic JFR for JFR viewers, the accounting report and
 * {@code jonoffcpu-offcpu-profile.pb}. Two slices of that profile are then rendered with the correlator's
 * {@code stacks} subcommand: {@code offcpu.collapsed} with every interval, and {@code offcpu-no-idle.collapsed}
 * without the intervals in which a thread was only waiting for work (see {@link #IDLE_WAITS_FILE}). Each slice has
 * a {@code .json} summary, which for the second accounts for what was removed, and an {@code .html} flame graph
 * with package names dropped so that a frame's box shows its class and method, rendered with the converter from
 * async-profiler's jonoffcpu fork, which comes as a dependency and takes {@code --units} so that the widths read
 * as microseconds rather than as sample counts.
 */
final class OffCpuFlamegraphs {
    static final String OUTPUT_SUFFIX = "-offcpu";
    static final String PROFILE_FILE = "jonoffcpu-offcpu-profile.pb";
    static final String ALL_SLICE = "offcpu";
    static final String NO_IDLE_SLICE = "offcpu-no-idle";
    private static final long SYNTHETIC_QUANTUM_NANOS = 10L * 1000 * 1000;

    /**
     * The frames that mark a thread waiting for work, one pattern per line, for {@code stacks --exclude-from}. The
     * file explains each group. It is copied into every output directory, so a run records the patterns it used.
     */
    static final String IDLE_WAITS_FILE = "offcpu-idle-waits.txt";

    private OffCpuFlamegraphs() {
    }

    /**
     * Correlates the samples of one recording that fall inside {@code [from, to)} and renders them.
     *
     * @param recording the complete JFR recording the agent wrote; it must be unmodified because the
     *                  capture stream binds its size and digest
     * @param from start of the window, inclusive
     * @param to end of the window, exclusive
     * @return the output directory
     */
    static Path process(Path recording, Instant from, Instant to) throws IOException, InterruptedException {
        Path stream = JonoffcpuAgent.capture(recording);
        if (!Files.isRegularFile(stream)) {
            throw new IOException("Recording " + recording + " has no capture stream " + stream);
        }
        String name = recording.getFileName().toString();
        Path outputDirectory = recording.resolveSibling(name.substring(0, name.length() - ".jfr".length())
                + OUTPUT_SUFFIX);
        if (Files.exists(outputDirectory)) {
            throw new IOException("Correlator output directory already exists: " + outputDirectory);
        }
        correlator("Correlating " + recording, outputDirectory, List.of(
                "--source", stream.toString(),
                "--jfr", recording.toString(),
                "--output", outputDirectory.toString(),
                "--from", Long.toString(from.toEpochMilli()),
                "--to", Long.toString(to.toEpochMilli()),
                // One synthetic event per quantum of matched off-CPU time. A broker waits for thousands of
                // seconds in a run, which at the default 1 ms makes the synthetic JFR ~100 MB; the collapsed
                // stacks and the profile keep the full resolution regardless.
                "--quantum-ns", Long.toString(SYNTHETIC_QUANTUM_NANOS),
                // The row-level audit files are by far the largest outputs, at about 2 KB per row, and nothing
                // here reads them. Every aggregate stays in jonoffcpu-report.json, and the capture stream is
                // kept, so correlating it again with --audit full reproduces them when a run needs examining.
                "--audit", "none"));
        Path profile = outputDirectory.resolve(PROFILE_FILE);
        if (Files.isRegularFile(profile)) {
            renderSlice(profile, ALL_SLICE, "Off-CPU time " + name, List.of());
            Path idleWaits = outputDirectory.resolve(IDLE_WAITS_FILE);
            try (InputStream patterns = OffCpuFlamegraphs.class.getResourceAsStream(IDLE_WAITS_FILE)) {
                if (patterns == null) {
                    throw new IOException("Missing launcher resource " + IDLE_WAITS_FILE);
                }
                Files.copy(patterns, idleWaits);
            }
            renderSlice(profile, NO_IDLE_SLICE, "Off-CPU time without idle waits " + name,
                    List.of("--exclude-from", idleWaits.toString()));
        }
        return outputDirectory;
    }

    /**
     * Renders one slice of the stack profile to {@code <slice>.collapsed}, {@code <slice>.json} and
     * {@code <slice>.html} beside it. The collapsed file keeps full names, which scripts, diff tools and
     * package-based classification need; only the flame graph drops package names.
     */
    private static void renderSlice(Path profile, String slice, String title, List<String> options)
            throws IOException, InterruptedException {
        Path directory = profile.getParent();
        Path collapsed = directory.resolve(slice + ".collapsed");
        List<String> arguments = new ArrayList<>(List.of("stacks", "--profile", profile.toString(),
                "--summary", directory.resolve(slice + ".json").toString(), "--output", collapsed.toString()));
        arguments.addAll(options);
        correlator("Rendering the " + slice + " slice of " + profile, directory, arguments);
        Path display = directory.resolve(slice + ".display.collapsed");
        try {
            // io.netty.channel.epoll.Native.epollWait0 becomes Native.epollWait0, so that a frame's box shows its
            // class and method. The filters still match the full names.
            List<String> displayArguments = new ArrayList<>(List.of("stacks", "--profile", profile.toString(),
                    "--package-names", "drop", "--output", display.toString()));
            displayArguments.addAll(options);
            correlator("Rendering the " + slice + " flame graph of " + profile, directory, displayArguments);
            render(display, directory.resolve(slice + ".html"), title);
        } finally {
            Files.deleteIfExists(display);
        }
    }

    private static void correlator(String action, Path outputDirectory, List<String> arguments)
            throws IOException, InterruptedException {
        int status;
        try {
            status = OffCpuCorrelator.run(arguments.toArray(String[]::new));
        } catch (IOException | InterruptedException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(action + " failed", e);
        }
        if (status != 0) {
            throw new IOException(action + " exited with status " + status + "; see " + outputDirectory);
        }
    }

    /**
     * Renders collapsed stacks in this JVM. The converter is the same code as the {@code jfr-converter.jar}
     * of a jonoffcpu release, so the arguments are its documented command line.
     */
    private static void render(Path collapsed, Path html, String title) throws IOException {
        try {
            Main.main(new String[] {"--title", title, "--units", "µs", collapsed.toString(), html.toString()});
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Rendering " + html + " failed", e);
        }
    }
}
