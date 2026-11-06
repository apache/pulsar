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

// The performance tests' metrics stack, VictoriaMetrics and Grafana, which Docker Compose runs from compose.yaml, and
// the scrape configuration of a run's containers. The launcher uses it for each run, and the up and down tasks start
// and stop the stack, to analyze runs in Grafana, see tests/performance/docs/metrics.md.
plugins {
    id("pulsar.java-conventions")
}

dependencies {
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.picocli)
    // The base URL of Grafana, as of the reports server
    implementation(project(":tests:performance:report-tool"))

    testImplementation(libs.assertj.core)
    testImplementation(libs.testng)
}

// The report tool it uses targets 21
tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

// The metrics stack's commands, see tests/performance/docs/metrics.md: up starts VictoriaMetrics, Grafana and Grafana's
// image renderer with Docker Compose in the background, and down stops them. -Pperformance.metrics.bindAddress sets the
// address that VictoriaMetrics and Grafana are published on, 127.0.0.1 by default, and
// -Pperformance.metrics.grafanaUrl the URL that up prints for Grafana.
fun JavaExec.configureMetricsCommand(command: String) {
    group = "verification"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.apache.pulsar.tests.performance.metrics.MetricsCommand")
    args(command)
    workingDir(rootProject.projectDir)
    systemProperty("performance.metrics.composeFile", layout.projectDirectory.file("compose.yaml").asFile.absolutePath)
    listOf("bindAddress", "grafanaUrl").forEach { name ->
        providers.gradleProperty("performance.metrics.$name").orNull?.let {
            systemProperty("performance.metrics.$name", it)
        }
    }
    outputs.upToDateWhen { false }
}

tasks.register<JavaExec>("up") {
    configureMetricsCommand("up")
    description = "Start the performance tests' metrics stack, VictoriaMetrics and Grafana, in the background"
}

tasks.register<JavaExec>("down") {
    configureMetricsCommand("down")
    description = "Stop the performance tests' metrics stack"
}
