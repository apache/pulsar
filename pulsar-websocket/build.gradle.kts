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
}

dependencies {
    implementation(libs.slog)
    api(project(":pulsar-broker-common"))
    implementation(project(":pulsar-common"))
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-docs-tools"))
    implementation(libs.commons.lang3)
    // guava was previously leaked onto the compile classpath via compileOnly(swagger-core 1.x)
    implementation(libs.guava)
    implementation(libs.jersey.container.servlet.core)
    implementation(libs.jersey.container.servlet)
    implementation(libs.jersey.hk2)
    implementation(libs.gson)
    implementation(libs.jackson.jakarta.rs.json.provider)
    implementation(libs.jetty.server)
    // ee10 + jakarta.servlet for the REST/admin tier (Jersey 3) and the WebSocket endpoint layer
    implementation(libs.jetty.ee10.servlet)
    implementation(libs.jetty.ee10.servlets)
    implementation(libs.jakarta.servlet.api)
    // Modern Jetty 12 WebSocket API on the ee10 stack (PIP-472)
    implementation(libs.jetty.ee10.websocket.jetty.server)
    implementation(libs.jetty.websocket.jetty.api)
    implementation(libs.jetty.websocket.jetty.client)
    implementation(libs.hdrHistogram)
    implementation(libs.picocli)
    implementation(libs.netty.common)
    implementation(libs.netty.buffer)

    compileOnly(libs.swagger.annotations)

    testImplementation(libs.netty.transport.native.epoll)
}
