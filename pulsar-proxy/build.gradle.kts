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

description = "Pulsar Proxy"

dependencies {
    api(project(":pulsar-broker-common"))
    api(project(":pulsar-client"))
    api(project(":pulsar-client-admin"))
    api(project(":pulsar-common"))
    api(project(":pulsar-opentelemetry"))
    api(project(":pulsar-docs-tools"))
    api(project(":pulsar-websocket"))

    implementation(libs.jetty.server)
    implementation("org.eclipse.jetty:jetty-alpn-conscrypt-server:${libs.versions.jetty.get()}")
    implementation(libs.jetty.ee8.servlet)
    implementation("org.eclipse.jetty.ee8:jetty-ee8-servlets:${libs.versions.jetty.get()}")
    implementation("org.eclipse.jetty.ee8:jetty-ee8-proxy:${libs.versions.jetty.get()}")
    implementation(libs.javax.servlet.api)
    implementation(libs.jersey.server)
    implementation("org.glassfish.jersey.containers:jersey-container-servlet-core:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.media:jersey-media-json-jackson:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.inject:jersey-hk2:${libs.versions.jersey.get()}")
    implementation("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:${libs.versions.jackson.get()}")
    implementation("jakarta.xml.bind:jakarta.xml.bind-api:${libs.versions.jakarta.xml.bind.get()}")
    implementation("com.sun.activation:jakarta.activation:${libs.versions.jakarta.activation.get()}")
    implementation(libs.guava)

    // Netty (proxy uses Netty directly for connection handling)
    implementation(libs.netty.handler)
    implementation(libs.netty.buffer)
    implementation(libs.netty.common)
    implementation(libs.netty.transport)
    implementation(libs.netty.codec)
    implementation(libs.netty.codec.haproxy)
    implementation(libs.netty.transport.native.epoll) {
        artifact { classifier = "linux-x86_64" }
    }
    implementation(libs.netty.resolver.dns)

    // BookKeeper
    implementation(libs.bookkeeper.common)

    // Websocket
    implementation("org.eclipse.jetty.ee8.websocket:jetty-ee8-websocket-jetty-server:${libs.versions.jetty.get()}")
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.simpleclient.servlet)
    implementation(libs.jackson.databind)
    implementation(libs.commons.lang3)
    implementation(libs.slf4j.api)
    implementation(libs.picocli)
    implementation(libs.log4j.core)
    implementation(libs.ipaddress)
    compileOnly(libs.swagger.annotations)

    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testArtifacts"))
    testImplementation(project(":buildtools"))
    testImplementation(project(":testmocks"))
    testImplementation(libs.awaitility)
    testImplementation(libs.testcontainers)
    testImplementation(libs.consolecaptor)
    testImplementation(libs.restassured)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation("org.eclipse.jetty.websocket:jetty-websocket-jetty-client:${libs.versions.jetty.get()}")
    testImplementation(libs.gson)
    testImplementation(libs.jjwt.impl)
    testImplementation(libs.jjwt.jackson)
    testImplementation(libs.okhttp3)
    testImplementation(libs.avro)
    testImplementation(libs.async.http.client)
}
