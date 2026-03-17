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

description = "Pulsar Functions Instance"

dependencies {
    api(project(":pulsar-functions:utils"))
    api(project(":pulsar-functions:api-java"))
    api(project(":pulsar-functions:secrets"))
    api(project(":pulsar-client"))
    api(project(":pulsar-client-admin"))
    api(project(":pulsar-common"))
    api(project(":pulsar-io-core"))
    api(project(":pulsar-metadata"))

    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(libs.grpc.stub)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.bookkeeper.common) {
        exclude(group = "io.grpc", module = "grpc-all")
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    implementation(libs.bookkeeper.stream.storage.java.client) {
        exclude(group = "io.grpc")
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.simpleclient.caffeine)
    implementation(libs.simpleclient.httpserver)
    implementation(libs.prometheus.jmx.collector)
    implementation(libs.guava)
    implementation(libs.caffeine)
    implementation(libs.picocli)
    implementation(libs.typetools)
    implementation(libs.sketches.core)
    implementation(libs.slf4j.api)
    implementation(libs.log4j.slf4j2.impl)
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    runtimeOnly(libs.perfmark.api)
    implementation(libs.bcprov.jdk18on)
    implementation(libs.bookkeeper.circe.checksum)

    testImplementation(project(":buildtools"))
    testImplementation(libs.awaitility)
}

tasks.test {
    environment("TEST_JAVA_INSTANCE_PARSE_ENV_VAR", "some-configuration")
}
