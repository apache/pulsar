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
    id("pulsar.public-java-library-conventions")
}

// Include parent module's test resources (YAML config files)
// This mirrors Maven's maven-antrun-plugin that copies pulsar-functions/src/test/resources/*.yml
sourceSets {
    test {
        resources {
            srcDir("${project.projectDir}/../src/test/resources")
        }
    }
}

tasks.named<ProcessResources>("processTestResources") {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

dependencies {
    implementation(libs.slog)
    api(project(":pulsar-functions:pulsar-functions-runtime"))
    api(project(":pulsar-broker-common"))
    implementation(project(":pulsar-opentelemetry"))
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-client-admin-original"))
    api(project(":pulsar-functions:pulsar-functions-proto"))
    implementation(project(":pulsar-functions:pulsar-functions-secrets"))
    implementation(project(":pulsar-docs-tools")) {
        exclude(group = "io.swagger.core.v3")
    }
    implementation(project(":pulsar-package-management:pulsar-package-core"))

    implementation(libs.commons.lang3)
    implementation(libs.commons.io)
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.picocli)
    api(libs.jackson.core)
    api(libs.jackson.databind)
    implementation(libs.netty.common)
    implementation(libs.byte.buddy)

    api(libs.distributedlog.core) {
        exclude(group = "net.jpountz.lz4", module = "lz4")
    }
    implementation(libs.bookkeeper.server)
    implementation(libs.zookeeper)

    implementation(libs.jersey.server)
    implementation(libs.jersey.container.servlet)
    implementation(libs.jersey.container.servlet.core)
    implementation(libs.jersey.media.json.jackson)
    api(libs.jersey.media.multipart)

    implementation(libs.jetty.server)
    implementation(libs.jetty.alpn.conscrypt.server)
    implementation(libs.jetty.ee10.servlet)
    implementation(libs.jetty.ee10.servlets)

    implementation(libs.jakarta.activation.api)
    api(libs.jakarta.ws.rs.api)

    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.simpleclient.common)
    implementation(libs.prometheus.metrics.model)
    implementation(libs.prometheus.metrics.exposition.textformats)

    compileOnly(libs.swagger.annotations)

    testImplementation(libs.protobuf.java.util)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(project(":pulsar-functions:pulsar-functions-api-examples"))
}

// NAR/JAR files needed by tests (mirrors Maven's maven-dependency-plugin config).
val testNars = projectNars("testNars",
    ":pulsar-functions:pulsar-functions-api-examples-builtin",
    ":pulsar-io:pulsar-io-data-generator")
val testExamplesJar = projectJars("testExamplesJar", ":pulsar-functions:pulsar-functions-api-examples")

tasks.withType<Test>().configureEach {
    // Inputs, which builds the archives before the tests run, and passed to the tests as system properties
    inputs.files(testNars.files).withPropertyName("testNars").withPathSensitivity(PathSensitivity.NONE)
    inputs.files(testExamplesJar.files).withPropertyName("testExamplesJar").withPathSensitivity(PathSensitivity.NONE)
    jvmArgumentProviders.add(TestArchivePaths(
        testNars.files.flatMap { it.incoming.artifacts.resolvedArtifacts },
        testExamplesJar.files.flatMap { it.incoming.artifacts.resolvedArtifacts },
        mapOf(
            "pulsar-functions-api-examples.nar.path" to ":pulsar-functions:pulsar-functions-api-examples-builtin",
            "pulsar-io-data-generator.nar.path" to ":pulsar-io:pulsar-io-data-generator",
        ),
        // A valid jar that is not a valid nar — used for invalid-nar tests
        listOf("pulsar-functions-api-examples.jar.path", "pulsar-io-invalid.nar.path"),
    ))
}
