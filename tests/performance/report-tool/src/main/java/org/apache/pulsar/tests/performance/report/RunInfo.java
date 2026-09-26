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
package org.apache.pulsar.tests.performance.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Where, by whom and from which code a run was made: the host and its hardware, the Docker engine that ran the
 * containers, the user, the project directory (which tells worktrees apart), the git branch and commit, and the
 * Pulsar version. The launcher collects it itself when a run starts, so it is the same whether the launcher runs
 * under Gradle or directly, and it describes the tree the run used rather than when {@code pulsar-common} was built,
 * as {@code pulsar-version.properties} would.
 *
 * <p>On a detached HEAD, the branch is the branch that contains the commit with the fewest commits after it, so
 * that a checked-out commit's runs sit with its branch's runs; {@code gitDetached} says that the HEAD was
 * detached. The branch is {@code HEAD} when no branch contains the commit.
 *
 * <p>Nothing here fails a run: a value that cannot be found is empty.
 */
public record RunInfo(ZonedDateTime started, String host, HostDetails hostDetails, DockerEngine dockerEngine,
               String user, String gitUserName, String gitUserEmail, Path projectDirectory, String gitBranch,
               boolean gitDetached, String gitCommit, boolean gitDirty, String version) {
    /** What {@code git rev-parse --abbrev-ref HEAD} prints on a detached HEAD. */
    static final String DETACHED_HEAD = "HEAD";

    static final String FILE_NAME = "run-info.json";

    private static final long GIT_TIMEOUT_SECONDS = 10;
    // JSON whatever mapper the caller reads its scenario with
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Collects the run's information, without the Docker engine, which {@link #withDockerEngine} adds.
     *
     * @param workingDirectory a directory inside the project, where git runs
     * @param started the run's start, which also names its directories
     */
    public static RunInfo collect(Path workingDirectory, ZonedDateTime started) {
        String topLevel = git(workingDirectory, "rev-parse", "--show-toplevel");
        Path projectDirectory = (topLevel.isEmpty() ? workingDirectory : Path.of(topLevel)).toAbsolutePath()
                .normalize();
        String commit = git(projectDirectory, "rev-parse", "HEAD");
        String branch = git(projectDirectory, "rev-parse", "--abbrev-ref", "HEAD");
        boolean detached = branch.equals(DETACHED_HEAD);
        if (detached) {
            branch = containingBranch(projectDirectory).orElse(DETACHED_HEAD);
        }
        return new RunInfo(started, hostName(), HostDetails.collect(), null, System.getProperty("user.name", ""),
                git(projectDirectory, "config", "user.name"), git(projectDirectory, "config", "user.email"),
                projectDirectory, branch, detached, commit,
                !commit.isEmpty() && !git(projectDirectory, "status", "--porcelain").isEmpty(),
                version(projectDirectory));
    }

    /** This information with the Docker engine that runs the containers, which is null when it is unknown. */
    public RunInfo withDockerEngine(DockerEngine engine) {
        return new RunInfo(started, host, hostDetails, engine, user, gitUserName, gitUserEmail, projectDirectory,
                gitBranch, gitDetached, gitCommit, gitDirty, version);
    }

    /**
     * Writes {@link #FILE_NAME}, with the keys of {@code pulsar-version.properties} where it has the same value, so
     * that tools can read either.
     */
    public void write(Path directory) throws IOException {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("started", started.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        values.put("version", version);
        values.put("git.commit.id", gitCommit);
        values.put("git.dirty", gitDirty);
        values.put("git.branch", gitBranch);
        values.put("git.detached", gitDetached);
        values.put("git.build.user.name", gitUserName);
        values.put("git.build.user.email", gitUserEmail);
        values.put("git.build.host", host);
        values.put("host.cpu", hostDetails.cpu());
        values.put("host.sockets", hostDetails.sockets());
        values.put("host.cores", hostDetails.cores());
        values.put("host.hardwareThreads", hostDetails.hardwareThreads());
        values.put("host.memoryBytes", hostDetails.memoryBytes());
        values.put("host.os", hostDetails.os());
        if (dockerEngine != null) {
            values.put("docker.version", dockerEngine.version());
            values.put("docker.cpus", dockerEngine.cpus());
            values.put("docker.memoryBytes", dockerEngine.memoryBytes());
            values.put("docker.os", dockerEngine.os());
            values.put("docker.kernel", dockerEngine.kernel());
            values.put("docker.architecture", dockerEngine.architecture());
        }
        values.put("user", user);
        values.put("projectDirectory", projectDirectory.toString());
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve(FILE_NAME).toFile(), values);
    }

    /**
     * The branch that contains HEAD with the fewest commits after it, a local branch before a remote-tracking one
     * at the same distance, and a remote-tracking branch named without its remote: a detached checkout of
     * {@code origin/master} is {@code master}. Git before 2.41 has no {@code ahead-behind}, and gets the branch with
     * the latest commit instead.
     */
    static Optional<String> containingBranch(Path projectDirectory) {
        String closest = git(projectDirectory, "for-each-ref", "--contains", "HEAD",
                "--format=%(ahead-behind:HEAD) %(symref) %(refname)", "refs/heads", "refs/remotes");
        if (!closest.isEmpty()) {
            return closestBranch(closest);
        }
        return latestBranch(git(projectDirectory, "for-each-ref", "--contains", "HEAD", "--sort=-committerdate",
                "--format=%(refname) %(symref)", "refs/heads", "refs/remotes"));
    }

    /**
     * The closest branch in {@code <ahead> <behind> <symref> <refname>} lines, where {@code <ahead>} is the number of
     * the branch's commits after HEAD.
     */
    static Optional<String> closestBranch(String forEachRefOutput) {
        return forEachRefOutput.lines()
                .map(line -> line.split(" "))
                // A symbolic ref, such as refs/remotes/origin/HEAD, has its target in the third field
                .filter(fields -> fields.length == 4 && fields[2].isEmpty())
                .min(Comparator.<String[]>comparingLong(fields -> Long.parseLong(fields[0]))
                        .thenComparing(fields -> fields[3].startsWith("refs/remotes/")))
                .map(fields -> branchName(fields[3]));
    }

    /**
     * The first branch in {@code <refname> <symref>} lines, newest first. A symbolic ref is told apart by its target
     * rather than by blank space, which {@link #git} trims from the start and the end of the output.
     */
    static Optional<String> latestBranch(String forEachRefOutput) {
        return forEachRefOutput.lines()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && line.indexOf(' ') < 0)
                .findFirst()
                .map(RunInfo::branchName);
    }

    // refs/heads/feat/x is feat/x, and refs/remotes/origin/feat/x is also feat/x
    private static String branchName(String refName) {
        String name = refName.substring(refName.indexOf('/', "refs/".length()) + 1);
        return refName.startsWith("refs/remotes/") ? name.substring(name.indexOf('/') + 1) : name;
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (IOException e) {
            String fromEnvironment = System.getenv("HOSTNAME");
            return fromEnvironment != null ? fromEnvironment : "";
        }
    }

    // The version the project builds, from the root gradle.properties as Gradle reads it
    private static String version(Path projectDirectory) {
        Path gradleProperties = projectDirectory.resolve("gradle.properties");
        if (!Files.isRegularFile(gradleProperties)) {
            return "";
        }
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(gradleProperties, StandardCharsets.ISO_8859_1)) {
            properties.load(reader);
        } catch (IOException e) {
            return "";
        }
        return properties.getProperty("version", "").trim();
    }

    /** The trimmed output of a git command, or empty when git is missing, fails or does not finish in time. */
    static String git(Path directory, String... arguments) {
        List<String> command = new ArrayList<>();
        command.add("git");
        command.addAll(List.of(arguments));
        try {
            Process process = new ProcessBuilder(command).directory(directory.toFile())
                    .redirectError(ProcessBuilder.Redirect.DISCARD).start();
            byte[] output;
            try (InputStream stdout = process.getInputStream()) {
                output = stdout.readAllBytes();
            }
            if (!process.waitFor(GIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return "";
            }
            return process.exitValue() == 0 ? new String(output, StandardCharsets.UTF_8).trim() : "";
        } catch (IOException e) {
            return "";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "";
        }
    }
}
