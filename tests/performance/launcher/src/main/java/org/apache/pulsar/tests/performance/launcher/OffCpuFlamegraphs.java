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

import io.github.jonoffcpu.correlator.OffCpuCorrelator;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
 * microseconds of off-CPU time), the accounting report, {@code jonoffcpu-offcpu-profile.pb} and the analysis digest
 * {@code jonoffcpu-summary.md}, which ranks the busy time, leaving out the idle waits of {@link #IDLE_WAITS_FILE}.
 * Four slices of that profile are then rendered with the correlator's {@code stacks} subcommand:
 * {@code offcpu} with every interval, {@code offcpu-no-idle} without the intervals in which a thread was only
 * waiting for work, and the same two rooted at the application's first frame ({@link #APPLICATION_ROOT}). Each
 * slice has a {@code .collapsed} file with full names, a {@code .json} summary, which for the no-idle slices
 * accounts for what was removed, and an {@code .html} flame graph with abbreviated package names and the
 * application's frames highlighted, rendered with the converter from async-profiler's jonoffcpu fork, which comes
 * as a dependency and takes {@code --units} so that the widths read as microseconds rather than as sample counts.
 */
final class OffCpuFlamegraphs {
    static final String OUTPUT_SUFFIX = "-offcpu";
    static final String PROFILE_FILE = "jonoffcpu-offcpu-profile.pb";
    static final String SUMMARY_FILE = "jonoffcpu-summary.md";
    static final String ALL_SLICE = "offcpu";
    static final String NO_IDLE_SLICE = "offcpu-no-idle";
    static final String APP_ROOT_SLICE = "offcpu-app-root";
    static final String NO_IDLE_APP_ROOT_SLICE = "offcpu-no-idle-app-root";
    static final List<String> SLICES = List.of(ALL_SLICE, NO_IDLE_SLICE, APP_ROOT_SLICE, NO_IDLE_APP_ROOT_SLICE);

    /**
     * The first frame of the application, for {@code stacks --root-at}: each stack starts at its root-most
     * matching frame, so application code reached through different thread pools, event loops or executors joins
     * into one tree instead of appearing at different depths under each infrastructure root. A stack without
     * such a frame becomes the single frame {@code [no application frame]}; the totals stay the same.
     */
    static final String APPLICATION_ROOT = "^org\\.apache\\.";

    /** The application's frames in the flame graphs, whose package names are abbreviated ({@code o.a.p.…}). */
    private static final String APPLICATION_HIGHLIGHT = "^o\\.a\\.";

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
        // The correlator creates its output directory, so the patterns it reads go beside the recording. They stay
        // there: the digest names that file in its reproduce commands.
        Path recordingIdleWaits = recording.resolveSibling(name.substring(0, name.length() - ".jfr".length())
                + "." + IDLE_WAITS_FILE);
        try (InputStream patterns = OffCpuFlamegraphs.class.getResourceAsStream(IDLE_WAITS_FILE)) {
            if (patterns == null) {
                throw new IOException("Missing launcher resource " + IDLE_WAITS_FILE);
            }
            Files.copy(patterns, recordingIdleWaits, StandardCopyOption.REPLACE_EXISTING);
        }
        correlator("Correlating " + recording, outputDirectory, List.of(
                "--source", stream.toString(),
                "--jfr", recording.toString(),
                "--output", outputDirectory.toString(),
                "--from", Long.toString(from.toEpochMilli()),
                "--to", Long.toString(to.toEpochMilli()),
                // The digest ranks busy time; these are the waits for work it leaves out. The collapsed stacks,
                // the profile and the report keep every interval.
                "--idle-from", recordingIdleWaits.toString(),
                // The row-level audit files are by far the largest outputs, at about 2 KB per row, and nothing
                // here reads them. Every aggregate stays in jonoffcpu-report.json, and the capture stream is
                // kept, so correlating it again with --audit full reproduces them when a run needs examining.
                "--audit", "none"));
        Path idleWaits = outputDirectory.resolve(IDLE_WAITS_FILE);
        Files.copy(recordingIdleWaits, idleWaits);
        Path profile = outputDirectory.resolve(PROFILE_FILE);
        if (Files.isRegularFile(profile)) {
            List<String> noIdle = List.of("--exclude-from", idleWaits.toString());
            List<String> appRoot = List.of("--root-at", APPLICATION_ROOT);
            renderSlice(profile, ALL_SLICE, "Off-CPU time " + name, List.of());
            renderSlice(profile, NO_IDLE_SLICE, "Off-CPU time without idle waits " + name, noIdle);
            renderSlice(profile, APP_ROOT_SLICE, "Off-CPU time from the application's first frame " + name,
                    appRoot);
            List<String> noIdleAppRoot = new ArrayList<>(noIdle);
            noIdleAppRoot.addAll(appRoot);
            renderSlice(profile, NO_IDLE_APP_ROOT_SLICE,
                    "Off-CPU time without idle waits from the application's first frame " + name, noIdleAppRoot);
        }
        return outputDirectory;
    }

    /**
     * Renders one slice of the stack profile to {@code <slice>.collapsed}, {@code <slice>.json} and
     * {@code <slice>.html} beside it. The collapsed file keeps full names, which scripts, diff tools and
     * package-based classification need; only the flame graph abbreviates package names.
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
            // io.netty.channel.epoll.Native.epollWait0 becomes i.n.c.e.Native.epollWait0, so that a frame's box
            // shows its class and method and the application's frames can still be told apart. The filters and
            // transforms still match the full names.
            List<String> displayArguments = new ArrayList<>(List.of("stacks", "--profile", profile.toString(),
                    "--package-names", "abbreviate", "--output", display.toString()));
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
            Main.main(new String[] {"--title", title, "--units", "µs", "--highlight", APPLICATION_HIGHLIGHT,
                    collapsed.toString(), html.toString()});
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Rendering " + html + " failed", e);
        }
    }
}
