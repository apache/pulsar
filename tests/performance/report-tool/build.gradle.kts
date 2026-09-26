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

// The reports and renderings of performance runs: the run and profile reports and their HTML pages, the latency,
// throughput and backlog charts, and the off-CPU and async-profiler flame graphs. The launcher runs the scenarios
// and calls into this module once a run has finished.
plugins {
    id("pulsar.java-conventions")
}

dependencies {
    // The report inputs are JSON trees (RunReport.Run) written with the caller's mapper
    api(libs.jackson.databind)
    // renderHdrHistograms names the applications from a run's resolved-config.yaml
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.hdrHistogram)
    implementation(libs.picocli)
    // Draws the latency charts as PNG (Apache-2.0). Its optional dependencies, such as the LGPL VectorGraphics2D
    // behind its SVG and PDF export, are not pulled in; only PNG export is used.
    implementation(libs.tooling.xchart)
    // Renders the Markdown reports to HTML pages whose links can be followed
    implementation(libs.tooling.commonmark)
    implementation(libs.tooling.commonmark.ext.gfm.tables)
    implementation(libs.tooling.commonmark.ext.heading.anchor)
    // Joins each recording with its off-CPU capture stream after a profiled run
    implementation(libs.tooling.jonoffcpu.correlator)
    // async-profiler's converter, from the fork that labels flame graph widths in microseconds. Having it
    // as a dependency is what keeps an async-profiler installation out of the profiling flow.
    implementation(libs.tooling.jonoffcpu.jfr.converter)
}

// The jonoffcpu correlator's published metadata requires Java 21.
tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

tasks.register<JavaExec>("renderHdrHistograms") {
    group = "verification"
    description = "Plot IoT publish and per-application end-to-end latencies by percentile and over time as PNG"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.apache.pulsar.tests.performance.report.HdrHistogramRenderer")
}

// Serves the reports root over HTTP. -Pperformance.reportsDir chooses the root as for the launcher's tasks, and
// -Pperformance.reportsServer.address and -Pperformance.reportsServer.port where to listen; all three can be set in
// ~/.gradle/gradle.properties. The loopback address by default keeps the reports off the network; reach them from
// another machine through an SSH tunnel.
tasks.register<JavaExec>("serveReports") {
    group = "verification"
    description = "Serve the performance reports over HTTP, at http://127.0.0.1:8000/ by default"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.apache.pulsar.tests.performance.report.ReportsServer")
    val reportsDir = providers.gradleProperty("performance.reportsDir").map { rootProject.file(it) }
        .getOrElse(rootProject.file("build/performance"))
    args("--directory", reportsDir.absolutePath,
        "--address", providers.gradleProperty("performance.reportsServer.address").getOrElse("127.0.0.1"),
        "--port", providers.gradleProperty("performance.reportsServer.port").getOrElse("8000"))
    outputs.upToDateWhen { false }
}
