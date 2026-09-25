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
val jonoffcpuAgent: Configuration by configurations.creating {
    isCanBeConsumed = false
    isTransitive = false
}

dependencies {
    implementation(project(":tests:performance:common"))
    implementation(project(path = ":tests:integration", configuration = "testJar"))
    implementation(libs.hdrHistogram)
    implementation(libs.picocli)
    // Samples topic backlog and message counters during a run for the run report
    implementation(project(":pulsar-client-admin-original"))
    // Renders the Markdown reports to HTML pages whose links can be followed
    implementation(libs.commonmark)
    implementation(libs.commonmark.ext.gfm.tables)
    implementation(libs.commonmark.ext.heading.anchor)
    // Joins each recording with its off-CPU capture stream after a profiled run
    implementation(libs.jonoffcpu.correlator)
    // async-profiler's converter, from the fork that labels flame graph widths in microseconds. Having it
    // as a dependency is what keeps an async-profiler installation out of the profiling flow.
    implementation(libs.jonoffcpu.jfr.converter)
    jonoffcpuAgent(libs.jonoffcpu.agent)
}

// The launcher already needs a recent JDK at run time (JfrCut uses the JDK 19+ recording writer), and the
// jonoffcpu correlator's published metadata requires Java 21, so the launcher targets 21.
tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

application {
    applicationName = "pulsar-performance-launcher"
    mainClass.set("org.apache.pulsar.tests.performance.launcher.PerformanceLauncher")
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
    systemProperty("performance.tools.dir",
        project(":tests:performance:tools").layout.buildDirectory.dir("install/pulsar-performance-tools")
            .get().asFile.absolutePath)
}

tasks.named<JavaExec>("run") {
    configurePerformanceLauncher(profiler = false)
    dependsOn(":tests:java-test-image:dockerBuild")
}

tasks.register<JavaExec>("profile") {
    group = "verification"
    description = "Run a standalone performance scenario with the jonoffcpu profiler (async-profiler plus " +
        "off-CPU samples) available in every container"
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

tasks.register<JavaExec>("renderHdrHistograms") {
    group = "verification"
    description = "Render IoT producer and consumer HDR latency histograms as PNG and SVG"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.apache.pulsar.tests.performance.launcher.HdrHistogramRenderer")
}
