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
package org.apache.pulsar.tests.integration.profiling;

import com.github.dockerjava.api.model.Driver;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.api.model.VolumeOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

/**
 * Attaches the <a href="https://github.com/lhotari/jonoffcpu">jonoffcpu</a> off-CPU profiler to a container.
 *
 * <p>The agent JAR embeds its own async-profiler build for both musl and glibc, so the container image does
 * not need one installed. The agent reads a small YAML file naming the async-profiler options, the JFR
 * recording path, the capture stream path and which off-CPU intervals to record. Both output paths are fixed
 * by the caller: unlike a plain async-profiler {@code file=} option, the capture path is not expanded and
 * must not exist before the JVM starts, and the agent binds the recording's size and digest into the stream
 * so the pair must stay together.
 *
 * <p>Off-CPU capture loads an eBPF program and signals the profiled threads, which needs the container to
 * run privileged and the JVM to run as root.
 */
public final class JonoffcpuAgent {
    /** Where the agent JAR is mounted inside the container. */
    public static final String AGENT_MOUNT = "/opt/jonoffcpu/jonoffcpu-agent.jar";
    /** Suffix of the capture stream written next to the {@code .jfr} recording. */
    public static final String CAPTURE_SUFFIX = ".jonoffcpu-capture.pb";
    /** Suffix of the agent configuration file written next to the recording. */
    public static final String CONFIG_SUFFIX = ".jonoffcpu.yaml";
    /** System property naming the agent JAR on the host, as the build passes it in. */
    public static final String AGENT_JAR_PROPERTY = "inttest.jonoffcpu.agent";
    /** Environment variable the test image's run scripts honour to run the Pulsar process as another user. */
    public static final String PROCESS_USER_ENV = "PULSAR_PROCESS_USER";
    /** The user the profiled JVM runs as. */
    public static final String ROOT_USER = "root";
    private static final String TRACEFS_MOUNT = "/sys/kernel/tracing";

    private JonoffcpuAgent() {
    }

    /**
     * Writes the agent configuration for one profiled JVM.
     *
     * @param hostDirectory the host directory that is bound at {@code containerDirectory} in the container
     * @param containerDirectory the same directory as the container sees it
     * @param baseName the recording name without extension; the JFR, capture stream and configuration
     *                 file are all derived from it
     * @param asyncProfilerOptions async-profiler options without a {@code file=} entry
     * @param sampling the agent's {@code sampling} block, which decides which off-CPU intervals are
     *                 recorded; the agent requires it, so it must not be empty
     * @return the JVM options to add to the container JVM's own
     */
    public static String writeConfig(Path hostDirectory, String containerDirectory, String baseName,
                                     String asyncProfilerOptions, Map<String, Object> sampling)
            throws IOException {
        if (asyncProfilerOptions.contains("file=")) {
            throw new IllegalArgumentException("Profiler options must not set file; the recording path is derived "
                    + "from the base name " + baseName);
        }
        if (sampling.isEmpty()) {
            // The agent has no default, so that a capture without off-CPU data is always a deliberate choice
            throw new IllegalArgumentException("jonoffcpu needs a sampling policy; set it in the scenario's "
                    + "profiling.offCpu section, or give it admission.policy: none to record only the "
                    + "async-profiler events");
        }
        List<String> lines = new ArrayList<>();
        lines.add("correlationOutput: " + quote(containerDirectory + "/" + baseName + CAPTURE_SUFFIX));
        lines.add("asyncProfilerOptions: " + quote(asyncProfilerOptions + ",file=" + containerDirectory + "/"
                + baseName + ".jfr"));
        lines.add("sampling:");
        appendBlock(lines, sampling, "  ");
        Path configFile = hostDirectory.resolve(baseName + CONFIG_SUFFIX);
        Files.write(configFile, lines, StandardCharsets.UTF_8);
        // The agent's protobuf codec writes the capture stream through sun.misc.Unsafe, which the JDK reports
        // once as a terminally deprecated call. The option silences that and changes nothing else.
        return "--sun-misc-unsafe-memory-access=allow -javaagent:" + AGENT_MOUNT + "=" + containerDirectory + "/"
                + configFile.getFileName();
    }

    /** Renders one level of the agent's YAML, nesting a map under its key. */
    private static void appendBlock(List<String> lines, Map<?, ?> values, String indent) {
        values.forEach((key, value) -> {
            if (value instanceof Map<?, ?> nested) {
                lines.add(indent + key + ":");
                appendBlock(lines, nested, indent + "  ");
            } else if (value instanceof List<?> items) {
                lines.add(indent + key + ": [" + String.join(", ", items.stream().map(JonoffcpuAgent::scalar)
                        .toList()) + "]");
            } else {
                lines.add(indent + key + ": " + scalar(value));
            }
        });
    }

    /**
     * A scalar as the agent's YAML wants it: numbers and booleans bare, and everything else quoted, which
     * keeps a probability such as {@code 0.010} spelled the way the capture metadata should record it.
     */
    private static String scalar(Object value) {
        return value instanceof Number || value instanceof Boolean ? value.toString() : quote(String.valueOf(value));
    }

    /**
     * Mounts the agent JAR read-only and grants what the eBPF collector needs: a privileged container running as
     * root, and a tracefs at {@code /sys/kernel/tracing}. Loading the programs needs {@code CAP_BPF} and
     * {@code CAP_PERFMON}, and Docker puts a container's capabilities in the effective set of root alone, which
     * is why the JVM runs as root. The scheduler hooks are BTF raw tracepoints, which a Linux host attaches
     * without tracefs; jonoffcpu still advises the mount for Docker Desktop, where that is unverified. A
     * container gets the tracefs directory but nothing mounted on it, so it is mounted read-only as a
     * Docker-managed volume; the Docker engine's kernel supplies it, without bind-mounting anything from the
     * host.
     *
     * <p>The image's own processes may still run as another user; see {@code PULSAR_PROCESS_USER} in the test
     * image's {@code func-lib.sh} for how the Pulsar containers switch supervisord's user.
     */
    public static void attach(GenericContainer<?> container, Path agentJar) {
        if (!Files.isRegularFile(agentJar)) {
            throw new IllegalArgumentException("jonoffcpu agent JAR not found: " + agentJar);
        }
        container.withFileSystemBind(agentJar.toAbsolutePath().toString(), AGENT_MOUNT, BindMode.READ_ONLY);
        container.withPrivilegedMode(true);
        container.withCreateContainerCmdModifier(cmd -> {
            cmd.withUser(ROOT_USER);
            HostConfig hostConfig = cmd.getHostConfig();
            List<Mount> mounts = new ArrayList<>();
            if (hostConfig.getMounts() != null) {
                mounts.addAll(hostConfig.getMounts());
            }
            mounts.add(new Mount()
                    .withType(MountType.VOLUME)
                    .withTarget(TRACEFS_MOUNT)
                    .withVolumeOptions(new VolumeOptions().withDriverConfig(new Driver()
                            .withName("local")
                            .withOptions(Map.of("type", "tracefs", "device", "tracefs", "o", "ro")))));
            hostConfig.withMounts(mounts);
        });
    }

    /**
     * The capture stream that belongs to a recording, following the naming used by {@link #writeConfig}.
     */
    public static Path capture(Path recording) {
        String name = recording.getFileName().toString();
        return recording.resolveSibling(name.substring(0, name.length() - ".jfr".length()) + CAPTURE_SUFFIX);
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
