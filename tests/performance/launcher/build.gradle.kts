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
    application
}

// The jonoffcpu agent JAR is mounted into profiled containers rather than loaded here, so it is
// resolved on its own instead of joining the launcher's classpath.
val jonoffcpuAgent = configurations.create("jonoffcpuAgent") {
    isCanBeConsumed = false
    isTransitive = false
}

dependencies {
    implementation(project(":tests:performance:common"))
    implementation(project(path = ":tests:integration", configuration = "testJar"))
    // Writes the run and profile reports, charts and flame graphs once a run has finished
    implementation(project(":tests:performance:report-tool"))
    // Checks the workload's settings with the workloads' own model before a cluster starts
    implementation(project(":tests:performance:tools"))
    // Collects the brokers' metrics into the metrics stack's VictoriaMetrics during a run
    implementation(project(":tests:performance:metrics"))
    implementation(libs.picocli)
    // Logs the stack traces of failures to launcher.log, and the console shows them in one line
    implementation(libs.slog)
    // Stops the logging, which closes launcher.log, before a successful run deletes it
    implementation(libs.log4j.api)
    // Merges the workloads' latency intervals for the progress lines
    implementation(libs.hdrHistogram)
    // Samples topic backlog and message counters during a run for the run report
    implementation(project(":pulsar-client-admin-original"))
    jonoffcpuAgent(libs.tooling.jonoffcpu.agent)
}

// The launcher already needs a recent JDK at run time (JfrCut uses the JDK 19+ recording writer), and the report
// tool it calls targets 21 for the jonoffcpu correlator, so the launcher targets 21.
tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

application {
    applicationName = "pulsar-performance-launcher"
    mainClass.set("org.apache.pulsar.tests.performance.launcher.PerformanceLauncher")
}

// ScenarioFilesTest resolves the scenario files that the performance tests ship, so that changing one reruns the tests
tasks.named<Test>("test") {
    inputs.dir(layout.projectDirectory.dir("../scenarios"))
        .withPropertyName("scenarios")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

// Profiled containers use the glibc-based Wolfi image: on musl every native frame reads as the unsymbolized
// /lib/ld-musl-x86_64.so.1, which hides what the JVM's own threads were waiting in. -Pinttest.testImageVariant=alpine
// profiles on the same Alpine image as every other run instead.
val wolfiTestImage = providers.gradleProperty("inttest.testImageVariant").map {
    when (it) {
        "wolfi" -> true
        "alpine" -> false
        else -> throw GradleException("inttest.testImageVariant must be alpine or wolfi, not '$it'")
    }
}.getOrElse(true)

fun JavaExec.configurePerformanceLauncher(profiler: Boolean) {
    workingDir(rootProject.projectDir)
    dependsOn(":tests:performance:tools:installDist")
    val imageSuffix = if (profiler && wolfiTestImage) "-wolfi" else ""
    environment("PULSAR_TEST_IMAGE_NAME",
        "${providers.gradleProperty("docker.organization").getOrElse("apachepulsar")}/java-test-image:"
            + providers.gradleProperty("docker.tag").getOrElse("latest") + imageSuffix)
    environment("PERFORMANCE_PROFILER_AVAILABLE", profiler.toString())
    // The scenario overrides, PULSAR_PERFORMANCE_<path> or pulsar_performance_<path>. The task's environment is a
    // snapshot that the configuration cache stores and replays, so the overrides are read as providers, which makes
    // them inputs of the cache: setting, changing or removing one reconfigures the task instead of replaying the
    // values of the run that stored the entry.
    val scenarioOverrides = providers.environmentVariablesPrefixedBy("PULSAR_PERFORMANCE_").get() +
        providers.environmentVariablesPrefixedBy("pulsar_performance_").get()
    environment(scenarioOverrides)
    systemProperty("performance.tools.dir",
        project(":tests:performance:tools").layout.buildDirectory.dir("install/pulsar-performance-tools")
            .get().asFile.absolutePath)
    // Root of the reports hierarchy, -Pperformance.reportsDir=<dir> (relative to the repository root, or
    // absolute). Without it the launcher writes to build/performance in the project directory it finds.
    providers.gradleProperty("performance.reportsDir").orNull?.let {
        systemProperty("performance.reports.dir", rootProject.file(it).absolutePath)
    }
    // The metrics stack that collects the brokers' metrics, see tests/performance/docs/metrics.md: its compose file,
    // to start VictoriaMetrics for a run when the stack doesn't run, and -Pperformance.metrics=false to collect none,
    // -Pperformance.metrics.bindAddress and -Pperformance.metrics.grafanaUrl as for its start task
    systemProperty("performance.metrics.composeFile",
        project(":tests:performance:metrics").layout.projectDirectory.file("compose.yaml").asFile.absolutePath)
    listOf("metrics", "metrics.bindAddress", "metrics.grafanaUrl").forEach { name ->
        providers.gradleProperty("performance.$name").orNull?.let { systemProperty("performance.$name", it) }
    }
    // The URL of the reports that :tests:performance:report-tool:serveReports serves, which the launcher prints beside
    // the reports: -Pperformance.reportsServer.baseUrl, else the server's bind address and port
    listOf("baseUrl", "bindAddress", "port").forEach { name ->
        providers.gradleProperty("performance.reportsServer.$name").orNull?.let {
            systemProperty("performance.reportsServer.$name", it)
        }
    }
    // Wait for the CPU package to cool down to this many °C before each run, -Pperformance.cooldownTemperature=<°C>,
    // so that runs start from comparable thermal conditions. Without it the launcher doesn't wait.
    providers.gradleProperty("performance.cooldownTemperature").orNull?.let {
        systemProperty("performance.cooldown.temperature", it)
    }
    // Keep launcher.log of a successful run, -Pperformance.keepLauncherLog. Without it the launcher deletes the log
    // when the run succeeds, since the containers' logs make it large; a failed run always keeps it. The property
    // alone, or with true, keeps it.
    providers.gradleProperty("performance.keepLauncherLog").orNull?.let {
        systemProperty("performance.keepLauncherLog", (it.isEmpty() || it.toBoolean()).toString())
    }
    // The cluster runs a released Pulsar, -Pperformance.clusterPulsarImage=<image> such as apachepulsar/pulsar:4.0.13,
    // in a test image that :tests:java-test-image:dockerBuildCluster builds on it, with the same tag: ZooKeeper, the
    // bookies and the brokers. The workloads keep this repository's test image, and with it its Pulsar client.
    providers.gradleProperty("performance.clusterPulsarImage").orNull?.let {
        dependsOn(":tests:java-test-image:dockerBuildCluster")
        systemProperty("performance.cluster.pulsarImage", it)
        systemProperty("performance.cluster.image",
            "${providers.gradleProperty("docker.organization").getOrElse("apachepulsar")}/java-test-image:cluster-"
                + it.replace(Regex("[^A-Za-z0-9_.-]"), "-").takeLast(120))
    }
}

tasks.named<JavaExec>("run") {
    configurePerformanceLauncher(profiler = false)
    dependsOn(":tests:java-test-image:dockerBuild")
}

tasks.register<JavaExec>("profile") {
    group = "verification"
    description = "Run a standalone performance scenario, profiling the JVMs that have profiler options with " +
        "async-profiler, JDK Flight Recorder and jonoffcpu's off-CPU recording at the same time"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set(application.mainClass)
    configurePerformanceLauncher(profiler = true)
    // The agent JAR embeds async-profiler, so nothing needs installing in the image. The cpu event still
    // samples through perf_events, and the off-CPU collector loads eBPF programs, so both need the
    // relaxed kernel limits.
    dependsOn(if (wolfiTestImage) ":tests:java-test-image:dockerBuildWolfi" else ":tests:java-test-image:dockerBuild",
        ":tests:integration:tuneKernelPerfEvents")
    inputs.files(jonoffcpuAgent)
    // The correlator sizes its retention budget at sixty percent of the heap and thins the capture, reporting
    // it, when a capture exceeds it. The heap is stated so that a run's result does not depend on how much
    // memory the host has; a few minutes of broker capture correlates in about 2 GB.
    maxHeapSize = providers.gradleProperty("performance.profile.maxHeapSize").getOrElse("4g")
    val agentJar = jonoffcpuAgent.elements.map { it.single().asFile.absolutePath }
    // The correlator reads the capture stream with a protobuf codec that uses sun.misc.Unsafe, which the
    // JDK reports once as a terminally deprecated call. The option silences that and changes nothing else.
    // It does not exist before JDK 24, so it is decided by the JVM that runs the task.
    val allowUnsafeMemoryAccess = JavaVersion.current() >= JavaVersion.VERSION_24
    // Providers rather than doFirst, so that the task can be stored in the configuration cache.
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOfNotNull("-Dperformance.jonoffcpu.agent=${agentJar.get()}",
            "--sun-misc-unsafe-memory-access=allow".takeIf { allowUnsafeMemoryAccess })
    })
    outputs.upToDateWhen { false }
    outputs.cacheIf("profiling runs are never cached") { false }
}

tasks.register<JavaExec>("runJfrCut") {
    group = "verification"
    description = "Inspect or cut a JFR recording to an absolute or recording-relative time interval"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.apache.pulsar.tests.performance.launcher.JfrCut")
}
