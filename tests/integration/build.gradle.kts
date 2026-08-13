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
val copyCertificateAuthority by tasks.registering(Copy::class) {
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
val ideaActive = providers.systemProperty("idea.active").map { it.toBoolean() }.getOrElse(false)
// When `--tests` is passed on the CLI, let TestNG discover tests directly from the classpath
// instead of restricting discovery to the suite XML — unless -PintegrationTestSuiteFile was
// set explicitly, in which case the user-selected suite still wins.
val hasCliTestsFilter = gradle.startParameter.taskRequests
    .flatMap { it.args }
    .any { it == "--tests" }
val integrationTest by tasks.registering(Test::class) {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath

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

    systemProperty("currentVersion", project.version.toString())
    systemProperty("buildDirectory", layout.buildDirectory.get().asFile.absolutePath)

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
