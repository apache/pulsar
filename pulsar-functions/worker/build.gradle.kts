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

description = "Pulsar Functions Worker"

dependencies {
    api(project(":pulsar-functions:runtime"))
    api(project(":pulsar-broker-common"))
    api(project(":pulsar-client-admin"))
    api(project(":pulsar-client"))
    api(project(":pulsar-common"))
    api(project(":pulsar-functions:proto"))
    api(project(":pulsar-opentelemetry"))
    api(project(":pulsar-docs-tools"))

    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.guava)
    implementation(libs.slf4j.api)
    implementation(libs.commons.io)
    implementation(libs.jersey.server)
    implementation("org.glassfish.jersey.containers:jersey-container-servlet:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.containers:jersey-container-servlet-core:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.media:jersey-media-json-jackson:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.media:jersey-media-multipart:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.inject:jersey-hk2:${libs.versions.jersey.get()}")
    implementation("jakarta.activation:jakarta.activation-api:${libs.versions.jakarta.activation.get()}")
    implementation(libs.jetty.server)
    implementation("org.eclipse.jetty:jetty-alpn-conscrypt-server:${libs.versions.jetty.get()}")
    implementation(libs.jetty.ee8.servlet)
    implementation("org.eclipse.jetty.ee8:jetty-ee8-servlets:${libs.versions.jetty.get()}")
    implementation(libs.bookkeeper.server) {
        exclude(group = "org.bouncycastle")
        exclude(group = "io.netty")
        exclude(group = "org.apache.zookeeper")
    }
    implementation(libs.bookkeeper.stream.storage.java.client) {
        exclude(group = "io.grpc", module = "grpc-all")
        exclude(group = "io.grpc", module = "grpc-okhttp")
    }
    implementation(libs.swagger.core)
    implementation(libs.gson)
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.picocli)
    implementation(libs.commons.lang3)
    implementation(project(":pulsar-package-management:core"))
    implementation(libs.byte.buddy)
    implementation("org.apache.distributedlog:distributedlog-core:${libs.versions.bookkeeper.get()}") {
        exclude(group = "org.bouncycastle")
        exclude(group = "io.netty")
    }
    implementation(libs.simpleclient.common)
    implementation(libs.zookeeper) {
        exclude(group = "ch.qos.logback")
        exclude(group = "io.netty", module = "netty-tcnative")
    }

    testImplementation(project(":buildtools"))
    testImplementation(project(":testmocks"))
    testImplementation(libs.awaitility)
    testImplementation(project(":pulsar-functions:java-examples"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testArtifacts"))
    testImplementation(libs.mockito.core)
}

// Worker tests that require NAR files built by the NAR packaging plugin
// These tests need pulsar-functions-api-examples.nar which is only available after full build
tasks.test {
    exclude("**/FunctionApiV2ResourceTest*")
    exclude("**/FunctionApiV3ResourceTest*")
    exclude("**/SinkApiV3ResourceTest*")
    exclude("**/SourceApiV3ResourceTest*")
}
