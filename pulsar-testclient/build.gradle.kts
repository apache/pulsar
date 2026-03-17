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

description = "Pulsar Test Client"

dependencies {
    api(project(":pulsar-client"))
    api(project(":pulsar-client-admin"))
    api(project(":pulsar-broker"))
    api(project(":pulsar-cli-utils"))

    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(libs.picocli)
    implementation(libs.hdrHistogram)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.slf4j.api)
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.re2j)
    implementation(libs.commons.lang3)
    implementation(libs.commons.codec)
    implementation(libs.bookkeeper.server) {
        exclude(group = "org.bouncycastle")
        exclude(group = "io.netty")
    }
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.httpserver)
    implementation(libs.zookeeper) {
        exclude(group = "ch.qos.logback")
        exclude(group = "io.netty", module = "netty-tcnative")
    }
    implementation("org.eclipse.jetty.websocket:jetty-websocket-jetty-api:${libs.versions.jetty.get()}")
    implementation("org.eclipse.jetty.websocket:jetty-websocket-jetty-client:${libs.versions.jetty.get()}")
    implementation(libs.opentelemetry.sdk)
    implementation("io.opentelemetry:opentelemetry-exporter-prometheus:1.56.0-alpha")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:1.56.0")
    implementation("io.opentelemetry:opentelemetry-sdk-extension-autoconfigure:1.56.0")

    testImplementation(project(":testmocks"))
    testImplementation(project(":buildtools"))
    testImplementation(libs.awaitility)
    testImplementation(project(path = ":pulsar-broker", configuration = "testArtifacts"))
}
