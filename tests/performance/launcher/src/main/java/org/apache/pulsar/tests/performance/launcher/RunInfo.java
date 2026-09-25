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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Where, by whom and from which code a run was made: the host, the user, the project directory (which tells
 * worktrees apart), the git branch and commit, and the Pulsar version. The launcher collects it itself when a run
 * starts, so it is the same whether the launcher runs under Gradle or directly, and it describes the tree the run
 * used rather than when {@code pulsar-common} was built, as {@code pulsar-version.properties} would.
 *
 * <p>Nothing here fails a run: a value that cannot be found is empty.
 */
record RunInfo(ZonedDateTime started, String host, String user, String gitUserName, String gitUserEmail,
               Path projectDirectory, String gitBranch, String gitCommit, boolean gitDirty, String version) {
    static final String FILE_NAME = "run-info.json";

    private static final long GIT_TIMEOUT_SECONDS = 10;
    // JSON whatever mapper the caller reads its scenario with
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Collects the run's information.
     *
     * @param workingDirectory a directory inside the project, where git runs
     * @param started the run's start, which also names its directories
     */
    static RunInfo collect(Path workingDirectory, ZonedDateTime started) {
        String topLevel = git(workingDirectory, "rev-parse", "--show-toplevel");
        Path projectDirectory = (topLevel.isEmpty() ? workingDirectory : Path.of(topLevel)).toAbsolutePath()
                .normalize();
        String commit = git(projectDirectory, "rev-parse", "HEAD");
        return new RunInfo(started, hostName(), System.getProperty("user.name", ""),
                git(projectDirectory, "config", "user.name"), git(projectDirectory, "config", "user.email"),
                projectDirectory, git(projectDirectory, "rev-parse", "--abbrev-ref", "HEAD"), commit,
                !commit.isEmpty() && !git(projectDirectory, "status", "--porcelain").isEmpty(),
                version(projectDirectory));
    }

    /**
     * Writes {@link #FILE_NAME}, with the keys of {@code pulsar-version.properties} where it has the same value, so
     * that tools can read either.
     */
    void write(Path directory) throws IOException {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("started", started.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        values.put("version", version);
        values.put("git.commit.id", gitCommit);
        values.put("git.dirty", gitDirty);
        values.put("git.branch", gitBranch);
        values.put("git.build.user.name", gitUserName);
        values.put("git.build.user.email", gitUserEmail);
        values.put("git.build.host", host);
        values.put("user", user);
        values.put("projectDirectory", projectDirectory.toString());
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve(FILE_NAME).toFile(), values);
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
