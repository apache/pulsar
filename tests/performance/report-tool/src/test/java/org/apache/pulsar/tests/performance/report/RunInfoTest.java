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

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.stream.Stream;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RunInfoTest {
    private static final ZonedDateTime STARTED = ZonedDateTime.parse("2026-09-25T06:42:59+03:00");
    private Path directory;

    @BeforeMethod
    public void createDirectory() throws IOException {
        directory = Files.createTempDirectory("run-info-test").toRealPath();
    }

    @AfterMethod(alwaysRun = true)
    public void deleteDirectory() throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @Test
    public void collectsTheCheckoutFromAnySubdirectory() throws IOException {
        Path project = directory.resolve("project");
        Path module = Files.createDirectories(project.resolve("tests/performance"));
        Files.writeString(project.resolve("gradle.properties"), "# build\nversion=5.0.0-SNAPSHOT\n");
        git(project, "init", "-q", "-b", "lh-branch");
        git(project, "add", "gradle.properties");
        git(project, "-c", "user.name=Test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false",
                "commit", "-q", "--no-verify", "-m", "initial");
        String commit = RunInfo.git(project, "rev-parse", "HEAD");

        RunInfo clean = RunInfo.collect(module, STARTED);
        Files.writeString(project.resolve("uncommitted.txt"), "change");
        RunInfo dirty = RunInfo.collect(module, STARTED);

        assertThat(clean.projectDirectory()).isEqualTo(project);
        assertThat(clean.gitBranch()).isEqualTo("lh-branch");
        assertThat(clean.gitCommit()).hasSize(40);
        assertThat(clean.gitCommit()).isEqualTo(commit);
        assertThat(clean.gitDirty()).isFalse();
        assertThat(dirty.gitDirty()).isTrue();
        assertThat(clean.version()).isEqualTo("5.0.0-SNAPSHOT");
        assertThat(clean.started()).isEqualTo(STARTED);
        assertThat(clean.host()).isNotEmpty();
        assertThat(clean.user()).isEqualTo(System.getProperty("user.name"));
    }

    @Test
    public void leavesGitValuesEmptyOutsideARepository() throws IOException {
        Path outside = Files.createDirectories(directory.resolve("outside"));
        // A repository further up, such as the one this test runs in, must not be found
        Files.writeString(outside.resolve(".git"), "gitdir: /nonexistent\n");

        RunInfo info = RunInfo.collect(outside, STARTED);

        assertThat(info.projectDirectory()).isEqualTo(outside);
        assertThat(info.gitBranch()).isEmpty();
        assertThat(info.gitCommit()).isEmpty();
        assertThat(info.gitDirty()).isFalse();
        assertThat(info.version()).isEmpty();
    }

    @Test
    public void writesTheKeysOfPulsarVersionProperties() throws IOException {
        RunInfo info = new RunInfo(STARTED, "perf-host", "lari", "Lari Hotari", "lari@example.com",
                Path.of("/work/pulsar"), "lh-branch", "0123456789abcdef0123456789abcdef01234567", true,
                "5.0.0-SNAPSHOT");

        info.write(directory);

        // JSON, not the YAML the launcher reads scenarios with
        JsonNode json = new ObjectMapper().readTree(Files.readString(directory.resolve(RunInfo.FILE_NAME)));
        assertThat(Files.readString(directory.resolve(RunInfo.FILE_NAME))).startsWith("{");
        assertThat(json.path("started").asText()).isEqualTo("2026-09-25T06:42:59+03:00");
        assertThat(json.path("version").asText()).isEqualTo("5.0.0-SNAPSHOT");
        assertThat(json.path("git.commit.id").asText()).isEqualTo("0123456789abcdef0123456789abcdef01234567");
        assertThat(json.path("git.dirty").asBoolean()).isTrue();
        assertThat(json.path("git.branch").asText()).isEqualTo("lh-branch");
        assertThat(json.path("git.build.user.name").asText()).isEqualTo("Lari Hotari");
        assertThat(json.path("git.build.user.email").asText()).isEqualTo("lari@example.com");
        assertThat(json.path("git.build.host").asText()).isEqualTo("perf-host");
        assertThat(json.path("user").asText()).isEqualTo("lari");
        assertThat(json.path("projectDirectory").asText()).isEqualTo("/work/pulsar");
    }

    private static void git(Path directory, String... arguments) throws IOException {
        String[] command = new String[arguments.length + 1];
        command[0] = "git";
        System.arraycopy(arguments, 0, command, 1, arguments.length);
        try {
            Process process = new ProcessBuilder(command).directory(directory.toFile()).inheritIO().start();
            if (process.waitFor() != 0) {
                throw new SkipException("git " + String.join(" ", arguments) + " failed");
            }
        } catch (IOException e) {
            throw new SkipException("No git here: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
