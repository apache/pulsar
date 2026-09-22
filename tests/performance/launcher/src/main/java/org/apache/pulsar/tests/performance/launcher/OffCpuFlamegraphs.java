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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import one.convert.Main;
import org.apache.pulsar.tests.integration.profiling.JonoffcpuAgent;

/**
 * Turns a jonoffcpu capture into an off-CPU flame graph.
 *
 * <p>For a recording {@code profile.jfr} with its {@code profile.jonoffcpu-capture.pb} stream, the correlator
 * writes {@code profile-offcpu/} holding {@code jonoffcpu-offcpu-stacks.collapsed} (Java stacks weighted in
 * microseconds of off-CPU time), a synthetic JFR for JFR viewers, the accounting report and
 * {@code jonoffcpu-offcpu-profile.pb}, from which the correlator's {@code stacks} subcommand renders other
 * slices (kernel stacks, one switch-out reason, filtered stacks) without correlating again. The collapsed
 * stacks are then rendered to {@code offcpu.html} with the converter from async-profiler's jonoffcpu fork,
 * which comes as a dependency and takes {@code --units} so that the widths read as microseconds rather than
 * as sample counts.
 */
final class OffCpuFlamegraphs {
    static final String OUTPUT_SUFFIX = "-offcpu";
    static final String COLLAPSED_FILE = "jonoffcpu-offcpu-stacks.collapsed";
    static final String HTML_FILE = "offcpu.html";
    private static final long SYNTHETIC_QUANTUM_NANOS = 10L * 1000 * 1000;

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
    static Path process(Path recording, Instant from, Instant to, String title)
            throws IOException, InterruptedException {
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
        List<String> arguments = List.of(
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
                "--audit", "none");
        int status;
        try {
            status = OffCpuCorrelator.run(arguments.toArray(String[]::new));
        } catch (IOException | InterruptedException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Correlating " + recording + " failed", e);
        }
        if (status != 0) {
            throw new IOException("Correlating " + recording + " exited with status " + status
                    + "; see " + outputDirectory);
        }
        Path collapsed = outputDirectory.resolve(COLLAPSED_FILE);
        if (Files.isRegularFile(collapsed)) {
            render(collapsed, outputDirectory.resolve(HTML_FILE), title);
        }
        return outputDirectory;
    }

    /**
     * Renders the collapsed stacks in this JVM. The converter is the same code as the {@code jfr-converter.jar}
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
