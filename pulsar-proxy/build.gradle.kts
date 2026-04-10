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
    api(project(":pulsar-broker-common"))
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-common"))
    implementation(project(":pulsar-opentelemetry"))
    implementation(project(":pulsar-docs-tools"))
    implementation(project(":pulsar-websocket"))
    implementation(libs.commons.lang3)
    implementation(libs.jetty.server)
    implementation(libs.jetty.alpn.conscrypt.server)
    implementation(libs.jetty.client)
    implementation(libs.jetty.ee8.servlet)
    implementation(libs.jetty.ee8.servlets)
    implementation(libs.jetty.ee8.proxy)
    implementation(libs.jetty.ee8.websocket.jetty.server)
    implementation(libs.jersey.server)
    implementation(libs.jersey.container.servlet.core)
    implementation(libs.jersey.container.servlet)
    implementation(libs.jersey.media.json.jackson)
    implementation(libs.jersey.hk2)
    implementation(libs.guava)
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.simpleclient.servlet)
    implementation(libs.jackson.jaxrs.json.provider)
    implementation(libs.picocli)
    implementation(libs.log4j.core)
    implementation(libs.log4j.api)
    implementation(libs.ipaddress)
    implementation(libs.netty.handler)
    implementation(libs.netty.transport)
    implementation(libs.netty.buffer)
    implementation(libs.netty.common)
    implementation(libs.netty.codec.haproxy)
    implementation(libs.netty.codec.http)
    implementation(libs.netty.resolver.dns)
    implementation(libs.netty.transport.native.epoll)
    implementation(libs.netty.handler.proxy)
    implementation(libs.bookkeeper.common)

    compileOnly(libs.swagger.annotations)

    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testJar"))
    testImplementation(project(":pulsar-client-admin-original"))
    testImplementation(project(":testmocks"))
    testImplementation(libs.asynchttpclient)
    testImplementation(libs.avro)
    testImplementation(libs.consolecaptor)
    testImplementation(libs.gson)
    testImplementation(libs.jetty.websocket.jetty.api)
    testImplementation(libs.jetty.websocket.jetty.client)
    testImplementation(libs.jjwt.api)
    testImplementation(libs.jjwt.impl)
    testImplementation(libs.okhttp3)
    testImplementation(libs.testcontainers)
}
