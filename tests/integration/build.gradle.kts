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

plugins {
    id("pulsar.java-conventions")
}

dependencies {
    implementation(libs.slog)
    testImplementation(libs.gson)
    testImplementation(project(":pulsar-functions:pulsar-functions-api-examples"))
    testImplementation(project(":pulsar-broker"))
    testImplementation(project(":pulsar-broker-common"))
    testImplementation(project(path = ":pulsar-broker-common", configuration = "testJar"))
    testImplementation(project(":pulsar-common"))
    testImplementation(project(":pulsar-client-original"))
    testImplementation(project(":pulsar-client-api-v5"))
    testImplementation(project(":pulsar-client-v5"))
    testImplementation(project(":pulsar-client-admin-original"))
    testImplementation(project(":pulsar-proxy"))
    testImplementation(project(":managed-ledger"))
    testImplementation(project(":buildtools"))
    testImplementation(project(":testmocks"))
    testImplementation(project(":pulsar-functions:pulsar-functions-worker"))
    testImplementation(project(":pulsar-functions:pulsar-functions-instance"))
    testImplementation(project(":pulsar-functions:pulsar-functions-runtime"))
    testImplementation(project(":pulsar-functions:pulsar-functions-secrets"))
    testImplementation(libs.bookkeeper.server)
    testImplementation(libs.ant)
    testImplementation(libs.failsafe)
    testImplementation(libs.docker.java.core)
    testImplementation(libs.bcpkix.jdk18on)
    testImplementation(libs.jackson.databind)
    testImplementation(libs.jackson.dataformat.yaml)
    testImplementation(libs.avro)
    testImplementation(libs.awaitility)
    testImplementation(libs.restassured)
    testImplementation(libs.testcontainers.k3s)
    testImplementation(libs.jetty.websocket.jetty.client)
    testImplementation(libs.joda.time)
    testImplementation(libs.kubernetes.client.java) {
        exclude(group = "io.prometheus", module = "simpleclient_httpserver")
        exclude(group = "org.bouncycastle")
        exclude(group = "javax.annotation", module = "javax.annotation-api")
        exclude(group = "software.amazon.awssdk")
        // Swagger 1.x annotations on the generated k8s models are inert metadata; nothing reads them at runtime
        exclude(group = "io.swagger", module = "swagger-annotations")
    }
    testImplementation(libs.kubernetes.client.java.api.fluent) {
        exclude(group = "io.prometheus", module = "simpleclient_httpserver")
        exclude(group = "org.bouncycastle")
        exclude(group = "javax.annotation", module = "javax.annotation-api")
        exclude(group = "software.amazon.awssdk")
        // Swagger 1.x annotations on the generated k8s models are inert metadata; nothing reads them at runtime
        exclude(group = "io.swagger", module = "swagger-annotations")
    }
}

// Copy certificate-authority resources to test output
val copyCertificateAuthority = tasks.register<Copy>("copyCertificateAuthority") {
    from("${rootDir}/tests/certificate-authority")
    into(layout.buildDirectory.dir("resources/test/certificate-authority"))
}

tasks.named("processTestResources") {
    dependsOn(copyCertificateAuthority)
}

// Tests are skipped by default — only run when explicitly invoked via the integration test runner
tasks.test {
    enabled = false
}

// Register a task for each integration test suite
val integrationTestSuiteFileProperty = providers.gradleProperty("integrationTestSuiteFile")
val integrationTestSuiteFile = integrationTestSuiteFileProperty.getOrElse("pulsar.xml")
val integrationTestSuiteFileExplicit = integrationTestSuiteFileProperty.isPresent
val integrationTestGroups = providers.gradleProperty("testGroups").orNull
val integrationTestExcludedGroups = providers.gradleProperty("excludedTestGroups").orNull
val integrationTestAsyncProfilerDir = providers.gradleProperty("inttest.asyncprofiler.dir")
    .getOrElse(layout.buildDirectory.get().asFile.absolutePath)
// Stamped into the async-profiler output file names so that profiles recorded from different
// revisions can be told apart when they are compared. Resolved here rather than left to the caller:
// a value nobody passes only produces empty segments in the file name. `git rev-parse` is
// best-effort — a tree built without git, or without a .git directory, simply leaves the commit id
// out of the name — and -Pgit.commit.id.abbrev overrides it. Nothing has to pass a timestamp in;
// the profiler expands the run timestamp itself.
val gitCommitIdAbbrev = providers.gradleProperty("git.commit.id.abbrev").orNull
    ?: runCatching {
        providers.exec {
            commandLine("git", "rev-parse", "--short", "HEAD")
            isIgnoreExitValue = true
        }.standardOutput.asText.get().trim()
    }.getOrDefault("")
// Must match the image that :tests:java-test-image:dockerBuildWithAsyncProfiler tags.
val dockerOrganization = providers.gradleProperty("docker.organization").getOrElse("apachepulsar")
val dockerTag = providers.gradleProperty("docker.tag").getOrElse("latest")
val ideaActive = providers.systemProperty("idea.active").map { it.toBoolean() }.getOrElse(false)
// When `--tests` is passed on the CLI, let TestNG discover tests directly from the classpath
// instead of restricting discovery to the suite XML — unless -PintegrationTestSuiteFile was
// set explicitly, in which case the user-selected suite still wins.
val hasCliTestsFilter = gradle.startParameter.taskRequests
    .flatMap { it.args }
    .any { it == "--tests" }
// Shared by `integrationTest` and `profilingIntegrationTest`: everything that is a property of
// running this module's tests at all, rather than of how a particular task selects them.
fun Test.configureIntegrationTestDefaults() {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath

    systemProperty("currentVersion", project.version.toString())
    systemProperty("buildDirectory", layout.buildDirectory.get().asFile.absolutePath)
    systemProperty("inttest.asyncprofiler.dir", integrationTestAsyncProfilerDir)
    providers.gradleProperty("inttest.asyncprofiler.opts").orNull?.let {
        systemProperty("inttest.asyncprofiler.opts", it)
    }
    providers.gradleProperty("inttest.asyncprofiler.outputformat").orNull?.let {
        systemProperty("inttest.asyncprofiler.outputformat", it)
    }
    // Cluster components to attach async-profiler to. Empty here: `integrationTest` only profiles
    // when asked to, and a test that drives profiling itself sets the spec flags directly.
    systemProperty("inttest.asyncprofiler.components",
        providers.gradleProperty("inttest.asyncprofiler.components").getOrElse(""))
    systemProperty("git.commit.id.abbrev", gitCommitIdAbbrev)

    jvmArgs(
        "-XX:+ExitOnOutOfMemoryError",
        "-Xmx1G",
        "-XX:MaxDirectMemorySize=1G",
    )

    maxParallelForks = 1
    forkEvery = 0

    testLogging {
        events("passed", "skipped", "failed")
        showExceptions = true
        showStackTraces = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

val integrationTest = tasks.register<Test>("integrationTest") {
    configureIntegrationTestDefaults()

    if (!ideaActive && (!hasCliTestsFilter || integrationTestSuiteFileExplicit)) {
        useTestNG {
            suites("src/test/resources/${integrationTestSuiteFile}")
            if (!integrationTestGroups.isNullOrEmpty()) {
                includeGroups(integrationTestGroups)
            }
            if (!integrationTestExcludedGroups.isNullOrEmpty()) {
                excludeGroups(integrationTestExcludedGroups)
            }
        }
    }

    val failFastValue = providers.gradleProperty("testFailFast").getOrElse("true").toBoolean()
    failFast = failFastValue
    val defaultTestRetryCount = if (ideaActive) "0" else "1"
    systemProperty("testRetryCount", providers.gradleProperty("testRetryCount").getOrElse(defaultTestRetryCount))
    systemProperty("testFailFast", failFastValue.toString())
}

// Profiling an integration test used to take a documented sequence of image builds, exported
// environment variables and a privileged container run. `profilingIntegrationTest` below does all of
// it, so profiling a cluster is one command. See CONTRIBUTING.md.

// The async-profiler `cpu` engine samples through perf_events, which the kernel gates. These are the
// settings from https://github.com/async-profiler/async-profiler/blob/master/docs/Troubleshooting.md,
// applied to the kernel that runs the containers (the Docker VM on macOS, the host on Linux) by a
// privileged throwaway container. Skip it with -Pinttest.asyncprofiler.skipPerfEventTuning when the
// values are already set, when the Docker setup disallows privileged containers, or when profiling
// with an engine that does not need perf_events.
val skipPerfEventTuning = providers.gradleProperty("inttest.asyncprofiler.skipPerfEventTuning").isPresent
val tuneKernelPerfEvents = tasks.register<Exec>("tuneKernelPerfEvents") {
    description = "Relax the kernel perf_event limits that async-profiler's cpu engine needs"
    // Copied into a local: a task action that captured the script-level val would capture the
    // script object with it, which the configuration cache cannot serialize.
    val skip = skipPerfEventTuning
    onlyIf { !skip }
    // Same image as PulsarContainer.ALPINE_IMAGE_NAME
    commandLine(
        "docker", "run", "--rm", "--privileged",
        "--cap-add", "SYS_ADMIN", "--security-opt", "seccomp=unconfined",
        "alpine:3.24", "sh", "-c",
        "echo 1 > /proc/sys/kernel/perf_event_paranoid "
            + "&& echo 0 > /proc/sys/kernel/kptr_restrict "
            + "&& echo 1024 > /proc/sys/kernel/perf_event_max_stack "
            + "&& echo 2048 > /proc/sys/kernel/perf_event_mlock_kb"
    )
    // Best effort: profiling still works without it, only with less accurate native stacks, so a
    // Docker setup that refuses privileged containers must not fail the whole profiling run.
    isIgnoreExitValue = true
    doLast {
        if ((this as Exec).executionResult.get().exitValue != 0) {
            logger.warn("Could not relax the kernel perf_event limits. Profiling continues, but the "
                + "cpu engine may produce incomplete stacks. Set the values manually, or pass "
                + "-Pinttest.asyncprofiler.skipPerfEventTuning to skip this step.")
        }
    }
}

val profilingTestClass = "org.apache.pulsar.tests.integration.profiling.PulsarProfilingTest"
tasks.register<Test>("profilingIntegrationTest") {
    group = "verification"
    description = "Run $profilingTestClass with async-profiler enabled in the cluster containers. " +
        "Pass --tests to profile a different integration test."

    configureIntegrationTestDefaults()

    // Build the test image that carries the profiler, and prepare the kernel for it.
    dependsOn(":tests:java-test-image:dockerBuildWithAsyncProfiler", tuneKernelPerfEvents)

    // Target the test directly rather than through a suite XML, so that --tests can point this at
    // any other integration test without a suite file having to exist for it.
    if (!hasCliTestsFilter) {
        filter {
            includeTestsMatching(profilingTestClass)
        }
    }

    environment("PULSAR_TEST_IMAGE_NAME",
        "${dockerOrganization}/java-test-image:${dockerTag}-asyncprofiler")
    // Leak detection is paranoid by default and would distort the allocation profile.
    environment("NETTY_LEAK_DETECTION", "off")
    // PulsarProfilingTest is a manual test: it skips itself unless this is set.
    environment("ENABLE_MANUAL_TEST", "true")

    // Unlike `integrationTest`, this task exists to profile, so it profiles the broker unless told
    // otherwise. PulsarProfilingTest sets the spec flags itself and does not depend on this; every
    // other integration test does, since the flags default to off.
    systemProperty("inttest.asyncprofiler.components",
        providers.gradleProperty("inttest.asyncprofiler.components").getOrElse("broker"))
    // A retried test would profile the cluster twice into the same run.
    systemProperty("testRetryCount", "0")
    systemProperty("testFailFast", "true")
    failFast = true

    // A profiling run always has to run, and its result must never come from the build cache.
    outputs.upToDateWhen { false }
    outputs.cacheIf("profiling runs are never cached") { false }

    val profileDir = integrationTestAsyncProfilerDir
    doFirst {
        logger.lifecycle("Profiling with async-profiler into {}", profileDir)
    }
}
