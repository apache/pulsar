import java.net.InetAddress
import java.time.Instant

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
    id("pulsar.lightproto")
}

description = "Pulsar Common"

dependencies {
    api(project(":pulsar-client-api"))
    api(project(":pulsar-client-admin-api"))

    // Jackson
    implementation(libs.jackson.databind)
    implementation(libs.jackson.module.parameter.names)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.jackson.datatype.jdk8)
    implementation(libs.jackson.dataformat.yaml)

    // Swagger
    compileOnly(libs.swagger.annotations)

    // Logging
    implementation(libs.slf4j.api)

    // Google
    implementation(libs.guava)
    implementation(libs.re2j)
    implementation(libs.completable.futures)
    implementation(libs.gson)

    // Metrics
    implementation(libs.simpleclient.caffeine)

    // Netty
    implementation(libs.netty.handler)
    implementation(libs.netty.buffer)
    implementation(libs.netty.resolver.dns)
    implementation(libs.netty.transport.native.epoll) {
        artifact { classifier = "linux-x86_64" }
    }
    implementation(libs.netty.transport.native.epoll) {
        artifact { classifier = "linux-aarch_64" }
    }
    implementation(libs.netty.transport.native.unix.common)
    implementation(libs.netty.tcnative.boringssl.static)
    implementation(libs.netty.codec.haproxy)
    implementation(libs.netty.incubator.transport.classes.io.uring)
    implementation(libs.netty.incubator.transport.native.io.uring) {
        artifact { classifier = "linux-x86_64" }
    }
    implementation(libs.netty.incubator.transport.native.io.uring) {
        artifact { classifier = "linux-aarch_64" }
    }

    // BookKeeper
    implementation(libs.bookkeeper.common.allocator)
    implementation(libs.bookkeeper.cpu.affinity)
    implementation(libs.bookkeeper.circe.checksum)

    // Compression
    implementation(libs.aircompressor)

    // Jakarta
    implementation(libs.jakarta.ws.rs.api)

    // IO
    implementation(libs.commons.io)

    // Annotations
    compileOnly(libs.spotbugs.annotations)
    compileOnly(libs.jspecify)

    // Test
    testImplementation(libs.bc.fips)
    testImplementation(libs.lz4.java)
    testImplementation(libs.zstd.jni)
    testImplementation(libs.snappy.java)
    testImplementation(libs.awaitility)
    testImplementation(libs.jsonassert)
    testImplementation(project(":buildtools"))
}

// Generate PulsarVersion.java from template (replaces templating-maven-plugin)
val generateVersionClass = tasks.register("generateVersionClass") {
    val templateDir = file("src/main/java-templates")
    val outputDir = layout.buildDirectory.dir("generated/source/version/java")

    inputs.dir(templateDir)
    inputs.property("version", project.version)
    outputs.dir(outputDir)

    doLast {
        val outDir = outputDir.get().asFile
        templateDir.walkTopDown().filter { it.extension == "java" }.forEach { template ->
            val relativePath = template.relativeTo(templateDir)
            val outputFile = outDir.resolve(relativePath)
            outputFile.parentFile.mkdirs()

            // Get git info (best effort)
            val gitCommitId = try {
                Runtime.getRuntime().exec(arrayOf("git", "rev-parse", "HEAD"))
                    .inputStream.bufferedReader().readLine()?.trim() ?: ""
            } catch (_: Exception) { "" }
            val gitBranch = try {
                Runtime.getRuntime().exec(arrayOf("git", "rev-parse", "--abbrev-ref", "HEAD"))
                    .inputStream.bufferedReader().readLine()?.trim() ?: ""
            } catch (_: Exception) { "" }
            val gitDirty = try {
                Runtime.getRuntime().exec(arrayOf("git", "status", "--porcelain"))
                    .inputStream.bufferedReader().readLine() != null
            } catch (_: Exception) { false }

            var content = template.readText()
            content = content.replace("\${project.version}", project.version.toString())
            content = content.replace("\${git.commit.id}", gitCommitId)
            content = content.replace("\${git.dirty}", gitDirty.toString())
            content = content.replace("\${git.branch}", gitBranch)
            content = content.replace("\${git.build.user.email}", System.getProperty("user.name", "unknown"))
            content = content.replace("\${git.build.user.name}", System.getProperty("user.name", "unknown"))
            content = content.replace("\${git.build.host}", try {
                InetAddress.getLocalHost().hostName
            } catch (_: Exception) { "unknown" })
            content = content.replace("\${git.build.time}", Instant.now().toString())

            outputFile.writeText(content)
        }
    }
}

sourceSets.main {
    java.srcDir(layout.buildDirectory.dir("generated/source/version/java"))
}

tasks.named("compileJava") {
    dependsOn(generateVersionClass)
}

tasks.jar {
    manifest {
        attributes("Automatic-Module-Name" to "org.apache.pulsar.common")
    }
}
