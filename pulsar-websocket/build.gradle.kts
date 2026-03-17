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

description = "Pulsar WebSocket"

dependencies {
    api(project(":pulsar-broker-common"))
    api(project(":pulsar-client"))
    api(project(":pulsar-client-admin"))
    api(project(":pulsar-common"))
    api(project(":pulsar-docs-tools"))

    implementation(libs.jetty.server)
    implementation(libs.jetty.ee8.servlet)
    implementation("org.eclipse.jetty.ee8:jetty-ee8-servlets:${libs.versions.jetty.get()}")
    implementation(libs.javax.servlet.api)
    implementation("org.eclipse.jetty.websocket:jetty-websocket-jetty-api:${libs.versions.jetty.get()}")
    implementation("org.eclipse.jetty.ee8.websocket:jetty-ee8-websocket-jetty-server:${libs.versions.jetty.get()}")
    implementation("org.eclipse.jetty.websocket:jetty-websocket-jetty-client:${libs.versions.jetty.get()}")
    implementation(libs.jackson.databind)
    implementation("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:${libs.versions.jackson.get()}")
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.simpleclient)
    implementation(libs.slf4j.api)
    implementation(libs.commons.lang3)
    implementation(libs.picocli)
    implementation(libs.netty.common)
    implementation(libs.netty.buffer)
    implementation(libs.hdrHistogram)
    implementation("org.glassfish.jersey.containers:jersey-container-servlet-core:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.containers:jersey-container-servlet:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.inject:jersey-hk2:${libs.versions.jersey.get()}")
    compileOnly(libs.swagger.core)

    testImplementation(project(":managed-ledger"))
    testImplementation(project(":buildtools"))
    testImplementation(libs.awaitility)
    testImplementation(libs.netty.transport.native.epoll)
    testRuntimeOnly("org.eclipse.jetty:jetty-util:9.4.58.v20250814")
    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testArtifacts"))
}
