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

import java.net.InetAddress
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

plugins {
    id("pulsar.public-java-library-conventions")
    id("pulsar.test-certs-conventions")
    alias(libs.plugins.lightproto)
}

// Generates pulsar-version.properties as a *resource* (not a Java source) so that changes to
// git metadata only invalidate this module's processResources / jar tasks and do NOT trigger
// a recompile of pulsar-common or any downstream module's compileJava.
//
// Set `pulsar.includeBuildInfo=false` (e.g. in `~/.gradle/gradle.properties`) to skip generation
// entirely during development. PulsarVersion then returns placeholder values at runtime.
val includeBuildInfo = providers.gradleProperty("pulsar.includeBuildInfo")
    .map { it.toBoolean() }
    .orElse(true)

val generatePulsarBuildInfo by tasks.registering {
    description = "Generates pulsar-version.properties with version and (optionally) git/build metadata."
    val outputFile = layout.buildDirectory.file("generated-resources/buildinfo/org/apache/pulsar/pulsar-version.properties")
    val projectVersion = project.version.toString()
    val includeBuildInfoValue = includeBuildInfo

    // Lazy providers — evaluated at execution time only (no impact on configuration cache).
    val gitCommitId = providers.exec {
        commandLine("git", "rev-parse", "HEAD")
        isIgnoreExitValue = true
    }.standardOutput.asText.map { it.trim() }
    val gitDirty = providers.exec {
        commandLine("git", "status", "--porcelain")
        isIgnoreExitValue = true
    }.standardOutput.asText.map { it.isNotBlank().toString() }
    val gitBranch = providers.exec {
        commandLine("git", "rev-parse", "--abbrev-ref", "HEAD")
        isIgnoreExitValue = true
    }.standardOutput.asText.map { it.trim() }
    val gitUserEmail = providers.exec {
        commandLine("git", "config", "user.email")
        isIgnoreExitValue = true
    }.standardOutput.asText.map { it.trim() }
    val gitUserName = providers.exec {
        commandLine("git", "config", "user.name")
        isIgnoreExitValue = true
    }.standardOutput.asText.map { it.trim() }

    inputs.property("version", projectVersion)
    inputs.property("includeBuildInfo", includeBuildInfoValue)
    outputs.file(outputFile)

    doLast {
        val entries = linkedMapOf<String, String>()
        entries["version"] = projectVersion
        if (includeBuildInfoValue.get()) {
            entries["git.commit.id"] = gitCommitId.getOrElse("")
            entries["git.dirty"] = gitDirty.getOrElse("true")
            entries["git.branch"] = gitBranch.getOrElse("")
            entries["git.build.user.email"] = gitUserEmail.getOrElse("")
            entries["git.build.user.name"] = gitUserName.getOrElse("")
            entries["git.build.host"] = InetAddress.getLocalHost().hostName
            entries["git.build.time"] =
                ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
        }

        val outFile = outputFile.get().asFile
        outFile.parentFile.mkdirs()
        // Hand-rolled .properties writer so we don't get the non-deterministic timestamp comment
        // that java.util.Properties.store always emits. Values from git/InetAddress are ASCII
        // identifiers, so backslash escaping is sufficient.
        outFile.writeText(buildString {
            append("# Pulsar build info\n")
            entries.forEach { (key, value) ->
                append(key).append('=').append(value.replace("\\", "\\\\")).append('\n')
            }
        })
    }
}

sourceSets["main"].resources.srcDir(generatePulsarBuildInfo.map {
    layout.buildDirectory.dir("generated-resources/buildinfo").get()
})


dependencies {
    implementation(libs.slog)
    api(project(":pulsar-client-api"))
    api(project(":pulsar-client-admin-api"))

    implementation(libs.jackson.databind)
    implementation(libs.jackson.module.parameter.names)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.jackson.datatype.jdk8)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.guava)
    implementation(libs.simpleclient.caffeine)
    implementation(libs.jspecify)
    implementation(libs.netty.handler)
    implementation(libs.netty.buffer)
    implementation(libs.netty.resolver.dns)
    implementation(variantOf(libs.netty.transport.native.epoll) { classifier("linux-x86_64") })
    implementation(variantOf(libs.netty.transport.native.epoll) { classifier("linux-aarch_64") })
    implementation(libs.netty.transport.native.unix.common)
    implementation(libs.bookkeeper.common.allocator) {
        exclude(group = "commons-configuration", module = "commons-configuration2")
        exclude(group = "commons-beanutils", module = "commons-beanutils")
    }
    implementation(libs.bookkeeper.cpu.affinity) {
        exclude(group = "commons-configuration", module = "commons-configuration2")
        exclude(group = "commons-beanutils", module = "commons-beanutils")
    }
    implementation(libs.aircompressor)
    implementation(libs.bookkeeper.circe.checksum) {
        exclude(group = "io.netty")
        exclude(group = "commons-configuration", module = "commons-configuration2")
        exclude(group = "commons-beanutils", module = "commons-beanutils")
    }
    implementation(libs.netty.tcnative.boringssl.static)
    implementation(variantOf(libs.netty.tcnative.boringssl.static) { classifier("linux-x86_64") })
    implementation(variantOf(libs.netty.tcnative.boringssl.static) { classifier("linux-aarch_64") })
    implementation(variantOf(libs.netty.tcnative.boringssl.static) { classifier("osx-x86_64") })
    implementation(variantOf(libs.netty.tcnative.boringssl.static) { classifier("osx-aarch_64") })
    implementation(libs.netty.transport.classes.io.uring)
    implementation(variantOf(libs.netty.transport.native.io.uring) { classifier("linux-x86_64") })
    implementation(variantOf(libs.netty.transport.native.io.uring) { classifier("linux-aarch_64") })
    implementation(libs.netty.codec.haproxy)
    implementation(libs.commons.lang3)
    implementation(libs.jakarta.ws.rs.api)
    implementation(libs.commons.io)
    implementation(libs.re2j)
    implementation(libs.completable.futures)
    implementation(libs.gson)

    compileOnly(libs.swagger.annotations)
    compileOnly(libs.spotbugs.annotations)

    testImplementation(libs.bc.fips)
    testImplementation(libs.lz4.java)
    testImplementation(libs.zstd.jni)
    testImplementation(libs.snappy.java)
    testImplementation(libs.awaitility)
    testImplementation(libs.jsonassert)
}
