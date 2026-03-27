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
    alias(libs.plugins.protobuf)
}

dependencies {
    api(project(":pulsar-client-api"))
    implementation(project(":pulsar-common")) {
        exclude(group = "io.prometheus", module = "simpleclient_caffeine")
    }
    implementation(project(":bouncy-castle:bouncy-castle-bc"))
    compileOnly(project(":pulsar-client-messagecrypto-bc"))

    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.api.incubator)
    implementation(libs.netty.codec.http)
    implementation(libs.netty.handler.proxy)
    implementation(libs.netty.codec.socks)
    implementation(libs.netty.resolver.dns)
    implementation(variantOf(libs.netty.resolver.dns.native.macos) { classifier("osx-aarch_64") })
    implementation(variantOf(libs.netty.resolver.dns.native.macos) { classifier("osx-x86_64") })
    implementation(libs.guava)
    implementation(libs.bookkeeper.circe.checksum) {
        exclude(group = "io.netty")
    }
    implementation(libs.commons.lang3)
    implementation(libs.asynchttpclient)
    implementation(libs.netty.reactive.streams)
    implementation(libs.slf4j.api)
    implementation(libs.commons.codec)
    implementation(libs.sketches.core)
    implementation(libs.gson)
    implementation(libs.avro) {
        exclude(group = "org.slf4j")
    }
    implementation(libs.avro.protobuf) {
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    implementation(libs.jackson.module.jsonSchema)
    implementation(libs.jsr305)
    implementation(libs.jspecify)
    implementation(libs.roaringbitmap)

    compileOnly(libs.swagger.annotations)
    compileOnly(libs.protobuf.java)
    compileOnly(libs.joda.time)
    compileOnly(libs.spotbugs.annotations)

    testImplementation(libs.jsonassert)
    testImplementation(libs.awaitility)
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

// Generate Avro test classes from .avsc schema files
val avroTools by configurations.creating
dependencies {
    avroTools("org.apache.avro:avro-tools:${libs.versions.avro.get()}")
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
