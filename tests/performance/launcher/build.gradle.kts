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

dependencies {
    implementation(project(":tests:performance:common"))
    implementation(project(path = ":tests:integration", configuration = "testJar"))
    implementation(libs.picocli)
}

application {
    applicationName = "pulsar-performance-launcher"
    mainClass.set("org.apache.pulsar.tests.performance.launcher.PerformanceLauncher")
}

fun JavaExec.configurePerformanceLauncher(asyncProfiler: Boolean) {
    workingDir(rootProject.projectDir)
    dependsOn(":tests:performance:tools:installDist")
    val imageSuffix = if (asyncProfiler) "-asyncprofiler" else ""
    environment("PULSAR_TEST_IMAGE_NAME",
        "${providers.gradleProperty("docker.organization").getOrElse("apachepulsar")}/java-test-image:"
            + providers.gradleProperty("docker.tag").getOrElse("latest") + imageSuffix)
    environment("PERFORMANCE_ASYNC_PROFILER_AVAILABLE", asyncProfiler.toString())
    systemProperty("performance.tools.dir",
        project(":tests:performance:tools").layout.buildDirectory.dir("install/pulsar-performance-tools")
            .get().asFile.absolutePath)
}

tasks.named<JavaExec>("run") {
    configurePerformanceLauncher(asyncProfiler = false)
    dependsOn(":tests:java-test-image:dockerBuild")
}

tasks.register<JavaExec>("profile") {
    group = "verification"
    description = "Run a standalone performance scenario with async-profiler available in every container"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set(application.mainClass)
    configurePerformanceLauncher(asyncProfiler = true)
    dependsOn(":tests:java-test-image:dockerBuildWithAsyncProfiler", ":tests:integration:tuneKernelPerfEvents")
    outputs.upToDateWhen { false }
    outputs.cacheIf("profiling runs are never cached") { false }
}

tasks.register<JavaExec>("runJfrCut") {
    group = "verification"
    description = "Inspect or cut a JFR recording to an absolute or recording-relative time interval"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.apache.pulsar.tests.performance.launcher.JfrCut")
}
