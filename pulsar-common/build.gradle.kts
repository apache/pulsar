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

val generatePulsarVersion by tasks.registering {
    val templateFile = file("src/main/java-templates/org/apache/pulsar/PulsarVersion.java")
    val outputDir = layout.buildDirectory.dir("generated-sources/java-templates")
    val projectVersion = project.version.toString()

    // Resolve all providers at configuration time for configuration cache compatibility
    val gitCommitId = providers.exec { commandLine("git", "rev-parse", "HEAD") }
        .standardOutput.asText.map { it.trim() }
    val gitDirty = providers.exec { commandLine("git", "status", "--porcelain") }
        .standardOutput.asText.map { it.isNotBlank().toString() }
    val gitBranch = providers.exec { commandLine("git", "rev-parse", "--abbrev-ref", "HEAD") }
        .standardOutput.asText.map { it.trim() }
    val gitUserEmail = providers.exec {
        commandLine("git", "config", "user.email")
        isIgnoreExitValue = true
    }.standardOutput.asText.map { it.trim() }
    val gitUserName = providers.exec {
        commandLine("git", "config", "user.name")
        isIgnoreExitValue = true
    }.standardOutput.asText.map { it.trim() }
    val buildHost = provider<String> { InetAddress.getLocalHost().hostName }
    val buildTime = provider<String> {
        ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
    }

    inputs.file(templateFile)
    inputs.property("version", projectVersion)
    // TODO: Replace git/build metadata with a Gradle configuration cache and build cache
    // compatible solution. Currently gitCommitId and gitDirty are not declared as inputs
    // because they cause cascading rebuilds of pulsar-common and all its dependents.
    // gitCommitId changes on every commit, and gitDirty changes whenever any file is
    // modified (via `git status --porcelain`). The values are still captured in the output
    // when the task runs for other reasons (same as buildTime/buildHost).
    outputs.dir(outputDir)

    doLast {
        val template = templateFile.readText()
        val generated = template
            .replace("\${project.version}", projectVersion)
            .replace("\${git.commit.id}", gitCommitId.getOrElse(""))
            .replace("\${git.dirty}", gitDirty.getOrElse("true"))
            .replace("\${git.branch}", gitBranch.getOrElse(""))
            .replace("\${git.build.user.email}", gitUserEmail.getOrElse(""))
            .replace("\${git.build.user.name}", gitUserName.getOrElse(""))
            .replace("\${git.build.host}", buildHost.get())
            .replace("\${git.build.time}", buildTime.get())

        val outFile = outputDir.get().file("org/apache/pulsar/PulsarVersion.java").asFile
        outFile.parentFile.mkdirs()
        outFile.writeText(generated)
    }
}

sourceSets["main"].java.srcDir(generatePulsarVersion.map { layout.buildDirectory.dir("generated-sources/java-templates").get() })


dependencies {
    api(project(":pulsar-client-api"))
    api(project(":pulsar-client-admin-api"))

    implementation(libs.slog)
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
    implementation(libs.netty.incubator.transport.classes.io.uring)
    implementation(variantOf(libs.netty.incubator.transport.native.io.uring) { classifier("linux-x86_64") })
    implementation(variantOf(libs.netty.incubator.transport.native.io.uring) { classifier("linux-aarch_64") })
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
