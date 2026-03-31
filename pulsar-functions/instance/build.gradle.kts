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
    api(project(":pulsar-functions:pulsar-functions-utils"))
    implementation(project(":pulsar-functions:pulsar-functions-api"))
    implementation(project(":pulsar-functions:pulsar-functions-secrets"))
    implementation(project(":pulsar-functions:pulsar-functions-proto"))
    implementation(project(":pulsar-metadata"))
    implementation(project(":pulsar-io:pulsar-io-core"))
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-client-admin-original"))
    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.commons.lang3)
    implementation(libs.caffeine)
    implementation(libs.picocli)
    implementation(libs.typetools)
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.simpleclient.caffeine)
    implementation(libs.simpleclient.httpserver)
    implementation(libs.prometheus.jmx.collector)
    implementation(libs.sketches.core)
    implementation(libs.jackson.databind)
    implementation(libs.netty.buffer)
    implementation(libs.netty.common)
    implementation(libs.bookkeeper.stream.storage.java.client) {
        exclude(group = "io.grpc")
        exclude(group = "com.google.protobuf")
    }
    implementation(libs.bookkeeper.common) {
        exclude(group = "io.grpc")
        exclude(group = "com.google.protobuf")
    }
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.all)
    runtimeOnly(libs.perfmark.api)
    implementation(libs.log4j.slf4j2.impl)
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.bcpkix.jdk18on)
    implementation(libs.bookkeeper.circe.checksum)
}

tasks.withType<Test> {
    environment("TEST_JAVA_INSTANCE_PARSE_ENV_VAR", "some-configuration")
}
