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
// `pulsarIncludeBuildInfo` (default `false` in the root gradle.properties) controls whether the
// git/build metadata is captured at all. Release builds enable it with
// `-PpulsarIncludeBuildInfo=true` or the `ORG_GRADLE_PROJECT_pulsarIncludeBuildInfo=true`
// environment variable. When disabled, PulsarVersion returns placeholder values at runtime.
val includeBuildInfo = providers.gradleProperty("pulsarIncludeBuildInfo")
    .map { it.toBoolean() }
    .orElse(true)

// `pulsarBuildInfoFile` (optional) points to a snapshot file that keeps the captured build
// metadata identical across separate Gradle invocations (the release process runs several).
// If the file exists, its entries are used as-is; otherwise the metadata is captured and
// written to the file so that subsequent invocations reuse it. Without the snapshot, values
// such as `git.build.time` change on every capture and invalidate the build outputs.
// A relative path is resolved against the root project directory.
val buildInfoFile = providers.gradleProperty("pulsarBuildInfoFile")
    .map { rootDir.resolve(it) }

val generatePulsarBuildInfo by tasks.registering {
    description = "Generates pulsar-version.properties with version and (optionally) git/build metadata."
    val outputFile = layout.buildDirectory.file("generated-resources/buildinfo/org/apache/pulsar/pulsar-version.properties")
    val projectVersion = project.version.toString()
    val includeBuildInfoValue = includeBuildInfo
    val buildInfoFileValue = buildInfoFile.orNull

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
    // The snapshot file contents take part in up-to-date checking so that pointing
    // `pulsarBuildInfoFile` at a different snapshot regenerates the resource.
    inputs.property("buildInfoFileContents", providers.fileContents(layout.file(buildInfoFile)).asText.orElse(""))
    outputs.file(outputFile)

    doLast {
        // Hand-rolled .properties writer so we don't get the non-deterministic timestamp comment
        // that java.util.Properties.store always emits. Values from git/InetAddress are ASCII
        // identifiers, so backslash escaping is sufficient.
        fun formatProperties(entries: Map<String, String>) = buildString {
            append("# Pulsar build info\n")
            entries.forEach { (key, value) ->
                append(key).append('=').append(value.replace("\\", "\\\\")).append('\n')
            }
        }

        val entries = linkedMapOf<String, String>()
        entries["version"] = projectVersion
        if (includeBuildInfoValue.get()) {
            val buildInfoEntries = linkedMapOf<String, String>()
            if (buildInfoFileValue != null && buildInfoFileValue.exists()) {
                // Reuse the previously captured snapshot so that the metadata stays identical
                // across the multiple Gradle invocations of a release build.
                buildInfoFileValue.readLines()
                    .filter { it.isNotBlank() && !it.startsWith("#") }
                    .forEach { line ->
                        val separator = line.indexOf('=')
                        if (separator > 0) {
                            val key = line.substring(0, separator)
                            // the version always comes from the project, never from the snapshot
                            if (key != "version") {
                                buildInfoEntries[key] = line.substring(separator + 1).replace("\\\\", "\\")
                            }
                        }
                    }
            } else {
                buildInfoEntries["git.commit.id"] = gitCommitId.getOrElse("")
                buildInfoEntries["git.dirty"] = gitDirty.getOrElse("true")
                buildInfoEntries["git.branch"] = gitBranch.getOrElse("")
                buildInfoEntries["git.build.user.email"] = gitUserEmail.getOrElse("")
                buildInfoEntries["git.build.user.name"] = gitUserName.getOrElse("")
                buildInfoEntries["git.build.host"] = InetAddress.getLocalHost().hostName
                buildInfoEntries["git.build.time"] =
                    ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
                if (buildInfoFileValue != null) {
                    buildInfoFileValue.parentFile?.mkdirs()
                    buildInfoFileValue.writeText(formatProperties(buildInfoEntries))
                }
            }
            entries += buildInfoEntries
        }

        val outFile = outputFile.get().asFile
        outFile.parentFile.mkdirs()
        outFile.writeText(formatProperties(entries))
    }
}

sourceSets["main"].resources.srcDir(generatePulsarBuildInfo.map {
    layout.buildDirectory.dir("generated-resources/buildinfo").get()
})


dependencies {
    implementation(libs.slog)
    api(project(":pulsar-client-api"))
    api(project(":pulsar-client-admin-api"))

    api(libs.jackson.databind)
    implementation(libs.jackson.module.parameter.names)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.jackson.datatype.jdk8)
    implementation(libs.jackson.dataformat.yaml)
    api(libs.guava)
    api(libs.simpleclient.caffeine)
    api(libs.jspecify)
    api(libs.netty.handler)
    api(libs.netty.buffer)
    api(libs.netty.resolver.dns)
    implementation(libs.roaringbitmap)
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
    api(libs.aircompressor)
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
    implementation(variantOf(libs.netty.tcnative.boringssl.static) { classifier("windows-x86_64") })
    implementation(libs.netty.transport.classes.io.uring)
    implementation(variantOf(libs.netty.transport.native.io.uring) { classifier("linux-x86_64") })
    implementation(variantOf(libs.netty.transport.native.io.uring) { classifier("linux-aarch_64") })
    api(libs.netty.codec.haproxy)
    implementation(libs.commons.lang3)
    api(libs.jakarta.ws.rs.api)
    implementation(libs.commons.io)
    implementation(libs.re2j)
    implementation(libs.completable.futures)
    api(libs.gson)

    compileOnly(libs.swagger.annotations)
    compileOnly(libs.spotbugs.annotations)

    // Non-FIPS BouncyCastle provider for tests that exercise SecurityUtility (which loads
    // org.bouncycastle.jce.provider.BouncyCastleProvider in a static initializer). This matches
    // the provider used in production. FIPS is covered separately by the bcfips-include-test
    // module; bc-fips must not be on a classpath that also has the non-FIPS provider because both
    // jars define org.bouncycastle.* and the JVM rejects the mismatched signers.
    testImplementation(libs.bcprov.jdk18on)
    testImplementation(libs.lz4.java)
    testImplementation(libs.zstd.jni)
    testImplementation(libs.snappy.java)
    testImplementation(libs.awaitility)
    testImplementation(libs.jsonassert)
}
