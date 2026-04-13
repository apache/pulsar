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
    id("pulsar.public-java-library-conventions")
    id("pulsar.test-certs-conventions")
}

dependencies {
    implementation(libs.slog)
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-client-admin-original"))
    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(project(":pulsar-broker"))
    implementation(project(":pulsar-cli-utils"))
    implementation(project(":pulsar-common"))
    implementation(project(":managed-ledger"))
    implementation(libs.picocli)
    implementation(libs.hdrHistogram)
    implementation(libs.jackson.databind)
    implementation(libs.guava)
    implementation(libs.commons.lang3)
    implementation(libs.commons.codec)
    implementation(libs.slf4j.api)
    implementation(libs.netty.buffer)
    implementation(libs.netty.common)
    implementation(libs.zookeeper) {
        exclude(group = "org.slf4j")
    }
    implementation(libs.bookkeeper.server)
    implementation(libs.re2j)
    implementation(libs.gson)
    implementation(libs.jetty.client)
    implementation(libs.jetty.websocket.jetty.api)
    implementation(libs.jetty.websocket.jetty.client)
    implementation(libs.jetty.util)
    implementation(libs.opentelemetry.sdk.extension.autoconfigure)

    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testJar"))
}
