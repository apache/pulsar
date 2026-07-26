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

import org.gradle.api.publish.tasks.GenerateModuleMetadata

plugins {
    id("pulsar.public-java-library-conventions")
    alias(libs.plugins.protobuf)
}

dependencies {
    api(project(":pulsar-client-api"))
    api(project(":pulsar-common")) {
        exclude(group = "io.prometheus", module = "simpleclient_caffeine")
    }
    // Non-FIPS BouncyCastle JCA provider, needed at runtime by pulsar-client-messagecrypto-bc.
    implementation(libs.bcprov.jdk18on)
    implementation(libs.bcpkix.jdk18on)
    compileOnly(project(":pulsar-client-messagecrypto-bc"))

    api(libs.opentelemetry.api)
    implementation(libs.opentelemetry.api.incubator)
    implementation(libs.netty.codec.http)
    implementation(libs.netty.handler.proxy)
    implementation(libs.netty.codec.socks)
    implementation(libs.netty.resolver.dns)
    implementation(variantOf(libs.netty.resolver.dns.native.macos) { classifier("osx-aarch_64") })
    implementation(variantOf(libs.netty.resolver.dns.native.macos) { classifier("osx-x86_64") })
    api(libs.guava)
    implementation(libs.bookkeeper.circe.checksum) {
        exclude(group = "io.netty")
    }
    implementation(libs.commons.lang3)
    api(libs.asynchttpclient)
    implementation(libs.netty.reactive.streams)
    api(libs.slog)
    implementation(libs.commons.codec)
    implementation(libs.datasketches.java)
    implementation(libs.gson)
    api(libs.avro) {
        exclude(group = "org.slf4j")
    }
    implementation(libs.avro.protobuf) {
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    implementation(libs.jackson.module.jsonSchema)
    api(libs.jspecify)
    implementation(libs.roaringbitmap)
    implementation(libs.fastutil)

    compileOnly(libs.swagger.annotations)
    compileOnly(libs.protobuf.java)
    compileOnly(libs.joda.time)
    compileOnly(libs.spotbugs.annotations)
    // JSR-305 (javax.annotation.*) is a compile-time/static-analysis-only annotation library with no
    // runtime use. Keep it off the runtime classpath so it is not bundled into the shaded client jar
    // (Pulsar 5.0 drops javax.* from the runtime; JSpecify covers nullness). Consumers that want to run
    // SpotBugs/FindBugs analysis can add jsr305 themselves.
    compileOnly(libs.jsr305)

    testImplementation(libs.jsonassert)
    testImplementation(libs.awaitility)
    testImplementation(libs.wiremock)
    testImplementation(libs.protobuf.java)
    testImplementation(libs.protobuf.java.util)
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.joda.time)
    testImplementation(project(":pulsar-client-messagecrypto-bc"))
    testImplementation(project(":pulsar-functions:pulsar-functions-proto"))
}

protobuf {
    protoc {
        val protocVersion = providers.gradleProperty("protobufVersion").getOrElse(libs.versions.protobuf.get())
        artifact = "com.google.protobuf:protoc:$protocVersion"
    }
}

// Only generate protobuf for test sources (no main protos)
sourceSets["main"].proto { exclude("**/*") }

// Align the protobuf plugin's resolvable proto-path classpaths with the enforced version platform.
// These build-time-only `<sourceSet>ProtoPath` configurations are never published, but the GitHub
// dependency-submission resolves every resolvable configuration and would otherwise report
// transitive versions diverging from the version catalog (e.g. netty-codec-http2), which Dependabot
// flags. See the matching comment in pulsar-broker/build.gradle.kts and pulsar.java-conventions.
configurations.matching { it.name.endsWith("ProtoPath") }.configureEach {
    extendsFrom(configurations["internalPlatform"])
}

// Generate Avro test classes from .avsc schema files
val avroTools by configurations.creating {
    // Align the avro-tools codegen classpath with the enforced version platform so its transitive
    // dependencies (commons-*, jetty-util, ...) match the version catalog instead of avro-tools'
    // own older transitives. Build-time only, never published.
    extendsFrom(configurations["internalPlatform"])
}
dependencies {
    avroTools("org.apache.avro:avro-tools:${libs.versions.avro.get()}") {
        exclude(group = "org.eclipse.jetty", module = "jetty-server")
    }
}

val generateTestAvro by tasks.registering(JavaExec::class) {
    val avscDir = file("src/test/resources/avro")
    val outputDir = layout.buildDirectory.dir("generated-sources/avro-test")
    inputs.dir(avscDir)
    outputs.dir(outputDir)
    classpath = avroTools
    mainClass.set("org.apache.avro.tool.Main")
    args("compile", "schema", avscDir.absolutePath, outputDir.get().asFile.absolutePath)
}

sourceSets["test"].java.srcDir(generateTestAvro.map { layout.buildDirectory.dir("generated-sources/avro-test").get() })
tasks.named("compileTestJava") { dependsOn(generateTestAvro) }

// ---------------------------------------------------------------------------
// Publish-time dependency replacement: in the PUBLISHED POM and Gradle Module Metadata only,
// replace the full fastutil dependency with the minimized :pulsar-client-fastutil-minimized jar,
// so Maven/Gradle consumers of pulsar-client-original pull in just the fastutil classes the client
// uses instead of the full ~25MB jar.
//
// This is publication-only on purpose: intra-build, pulsar-client-original keeps exposing the full
// fastutil (pulsar-broker depends on it and uses more fastutil classes than the client minimized
// set, so a build-scope swap would break the broker compile). There is deliberately no build-graph
// dependency on the minimized module either — it is built only when the shaded jars (or its own
// publication) are built, not every time a client class changes.
run {
    val fromGroup = "it.unimi.dsi"
    val fromName = "fastutil"
    val toGroup = "org.apache.pulsar"
    val toName = "pulsar-client-fastutil-minimized"
    val toVersion = version.toString()

    // POM (Maven consumers): rewrite the fastutil <dependency> block in place.
    publishing.publications.withType<MavenPublication>().configureEach {
        pom.withXml {
            val sb = asString()
            val replaced = sb.toString().replace(
                Regex(
                    "<dependency>\\s*<groupId>\\Q$fromGroup\\E</groupId>\\s*" +
                        "<artifactId>\\Q$fromName\\E</artifactId>.*?</dependency>",
                    RegexOption.DOT_MATCHES_ALL
                ),
                "<dependency>\n      <groupId>$toGroup</groupId>\n" +
                    "      <artifactId>$toName</artifactId>\n" +
                    "      <version>$toVersion</version>\n" +
                    "      <scope>runtime</scope>\n    </dependency>"
            )
            sb.setLength(0)
            sb.append(replaced)
        }
    }

    // Gradle Module Metadata (Gradle consumers): post-process the generated .module JSON, since GMM
    // has no per-dependency rewrite API. Runs as the tail of the generate task so the published (and
    // signed) file is the rewritten one. Captures only Providers/strings -> configuration-cache safe.
    tasks.withType<GenerateModuleMetadata>().configureEach {
        val moduleFile = outputFile
        doLast {
            val file = moduleFile.get().asFile
            @Suppress("UNCHECKED_CAST")
            val json = groovy.json.JsonSlurper().parse(file) as MutableMap<String, Any?>
            val variants = json["variants"] as? List<*> ?: emptyList<Any?>()
            for (variant in variants) {
                val deps = (variant as? Map<*, *>)?.get("dependencies") as? List<*> ?: continue
                for (dep in deps) {
                    @Suppress("UNCHECKED_CAST")
                    val d = dep as? MutableMap<String, Any?> ?: continue
                    if (d["group"] == fromGroup && d["module"] == fromName) {
                        d["group"] = toGroup
                        d["module"] = toName
                        d["version"] = linkedMapOf("requires" to toVersion)
                    }
                }
            }
            file.writeText(groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(json)))
        }
    }
}
