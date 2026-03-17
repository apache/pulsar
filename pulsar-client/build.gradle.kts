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
    id("com.google.protobuf")
}

// Maven artifactId is "pulsar-client-original"
description = "Pulsar Client Original"

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf3.get()}"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

dependencies {
    api(project(":pulsar-client-api"))
    api(project(":pulsar-common")) {
        exclude(group = "io.prometheus", module = "simpleclient_caffeine")
    }

    implementation(project(":bouncy-castle:bc"))
    compileOnly(project(":pulsar-client-messagecrypto-bc"))

    // OpenTelemetry
    compileOnly(libs.opentelemetry.api)
    compileOnly(libs.opentelemetry.api.incubator)

    // Netty
    implementation(libs.netty.codec.http)
    implementation(libs.netty.handler.proxy)
    implementation(libs.netty.codec.socks)
    implementation(libs.netty.resolver.dns)
    runtimeOnly(libs.netty.resolver.dns.native.macos.osx.aarch64) {
        artifact { classifier = "osx-aarch_64" }
    }
    runtimeOnly(libs.netty.resolver.dns.native.macos.osx.x8664) {
        artifact { classifier = "osx-x86_64" }
    }

    // Swagger
    compileOnly(libs.swagger.annotations)

    // Google
    implementation(libs.guava)

    // BookKeeper
    implementation(libs.bookkeeper.circe.checksum)

    // Commons
    implementation(libs.commons.lang3)
    implementation(libs.commons.codec)

    // HTTP
    implementation(libs.async.http.client) {
        exclude(group = "io.netty")
        exclude(group = "com.typesafe.netty", module = "netty-reactive-streams")
    }
    implementation(libs.netty.reactive.streams)

    // Logging
    implementation(libs.slf4j.api)

    // Data
    implementation(libs.sketches.core)
    implementation(libs.gson)
    implementation(libs.avro) {
        exclude(group = "org.apache.commons", module = "commons-compress")
    }
    implementation(libs.avro.protobuf) {
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }

    // Serialization
    implementation(libs.jackson.module.jsonSchema)

    // Protobuf (provided scope — users bring their own)
    compileOnly(libs.protobuf.java)

    // Collections
    implementation(libs.roaringbitmap)
    implementation(libs.fastutil)

    // Annotations
    compileOnly(libs.jsr305)
    compileOnly(libs.jspecify)
    compileOnly(libs.spotbugs.annotations)

    // Provided
    compileOnly("joda-time:joda-time:${libs.versions.joda.get()}")

    // Test
    testImplementation(project(":pulsar-functions:proto"))
    testImplementation(libs.jsonassert)
    testImplementation(libs.awaitility)
    testImplementation(project(":buildtools"))
    testImplementation(project(":pulsar-client-messagecrypto-bc"))
    testImplementation(libs.opentelemetry.api)
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation("com.google.protobuf:protobuf-java-util:${libs.versions.protobuf3.get()}")
    testImplementation(libs.joda.time)
}
