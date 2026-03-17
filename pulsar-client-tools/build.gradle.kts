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

description = "Pulsar Client Tools"

dependencies {
    api(project(":pulsar-client-tools-api"))
    api(project(":pulsar-client"))
    api(project(":pulsar-client-admin"))
    api(project(":pulsar-common"))
    api(project(":pulsar-client-admin-api"))
    api(project(":pulsar-cli-utils"))
    api(project(":pulsar-websocket"))

    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(libs.picocli)
    implementation(libs.picocli.shell.jline3)
    implementation(libs.jline3)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.avro)
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.slf4j.api)
    implementation(libs.commons.lang3)
    implementation(libs.commons.io)
    implementation(libs.commons.text)
    implementation(libs.async.http.client)
    implementation(libs.netty.reactive.streams)
    implementation(libs.conscrypt.openjdk.uber)
    implementation("org.javassist:javassist:${libs.versions.javassist.get()}")
    implementation("org.eclipse.jetty.websocket:jetty-websocket-jetty-api:${libs.versions.jetty.get()}")
    implementation("org.eclipse.jetty.websocket:jetty-websocket-jetty-client:${libs.versions.jetty.get()}")
    runtimeOnly(libs.jna)
    compileOnly(libs.swagger.core)

    testImplementation(project(":pulsar-functions:utils"))
    testImplementation(project(":pulsar-io:batch-discovery-triggerers"))
    testImplementation(project(":buildtools"))
    testImplementation(libs.awaitility)
}
