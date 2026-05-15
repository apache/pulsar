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
    implementation(project(":pulsar-broker-common"))
    implementation(project(":pulsar-opentelemetry"))
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-client-admin-original"))
    implementation(project(":pulsar-functions:pulsar-functions-proto"))
    implementation(project(":pulsar-functions:pulsar-functions-secrets"))
    implementation(project(":pulsar-docs-tools")) {
        exclude(group = "io.swagger")
    }
    implementation(project(":pulsar-package-management:pulsar-package-core"))

    implementation(libs.commons.lang3)
    implementation(libs.commons.io)
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.picocli)
    implementation(libs.jackson.core)
    implementation(libs.jackson.databind)
    implementation(libs.netty.common)
    implementation(libs.byte.buddy)

    implementation(libs.distributedlog.core) {
        exclude(group = "net.jpountz.lz4", module = "lz4")
    }
    implementation(libs.bookkeeper.server)
    implementation(libs.zookeeper)

    implementation(libs.jersey.server)
    implementation(libs.jersey.container.servlet)
    implementation(libs.jersey.container.servlet.core)
    implementation(libs.jersey.media.json.jackson)
    implementation(libs.jersey.media.multipart)

    implementation(libs.jetty.server)
    implementation(libs.jetty.alpn.conscrypt.server)
    implementation(libs.jetty.ee8.servlet)
    implementation(libs.jetty.ee8.servlets)

    implementation(libs.jakarta.activation.api)
    implementation(libs.jakarta.ws.rs.api)

    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.simpleclient.common)

    compileOnly(libs.swagger.core) {
        exclude(group = "com.fasterxml.jackson.core")
        exclude(group = "com.fasterxml.jackson.dataformat")
    }

    testImplementation(libs.protobuf.java.util)
    testImplementation(project(":pulsar-functions:pulsar-functions-api-examples"))
}

// NAR/JAR files needed by tests (mirrors Maven's maven-dependency-plugin config).
// Resolve through dependency configurations instead of cross-project task references.
val testNars by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    attributes {
        attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, "nar")
    }
}
val testExamplesJar by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
}

dependencies {
    testNars(project(":pulsar-functions:pulsar-functions-api-examples-builtin"))
    testNars(project(":pulsar-io:pulsar-io-data-generator"))
    testExamplesJar(project(":pulsar-functions:pulsar-functions-api-examples"))
}

tasks.withType<Test> {
    dependsOn(testNars, testExamplesJar)
    // Map resolved NAR/JAR files to system properties
    val narFiles = testNars.incoming.artifacts.resolvedArtifacts
    val jarFiles = testExamplesJar.incoming.artifacts.resolvedArtifacts
    doFirst {
        val narMap = narFiles.get().associate {
            val id = it.id.componentIdentifier as org.gradle.api.artifacts.component.ProjectComponentIdentifier
            id.projectPath to it.file.absolutePath
        }
        val examplesJarPath = jarFiles.get().first().file.absolutePath
        systemProperty("pulsar-functions-api-examples.jar.path", examplesJarPath)
        systemProperty("pulsar-functions-api-examples.nar.path",
            narMap[":pulsar-functions:pulsar-functions-api-examples-builtin"]!!)
        systemProperty("pulsar-io-data-generator.nar.path",
            narMap[":pulsar-io:pulsar-io-data-generator"]!!)
        // A valid jar that is not a valid nar — used for invalid-nar tests
        systemProperty("pulsar-io-invalid.nar.path", examplesJarPath)
    }
}
