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

// Maven artifactId is "pulsar-client-admin-original"
description = "Pulsar Client Admin Original"

dependencies {
    api(project(":pulsar-client-admin-api"))
    api(project(":pulsar-common"))
    api(project(":pulsar-client"))

    implementation(libs.jackson.databind)
    implementation(libs.jackson.module.jsonSchema)
    implementation(libs.javax.servlet.api)
    implementation(libs.jakarta.ws.rs.api)
    implementation(libs.jersey.server)
    implementation("org.glassfish.jersey.media:jersey-media-json-jackson:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.inject:jersey-hk2:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.containers:jersey-container-servlet-core:${libs.versions.jersey.get()}")
    implementation(libs.async.http.client) {
        exclude(group = "io.netty")
        exclude(group = "com.typesafe.netty")
    }
    implementation(libs.netty.reactive.streams)
    implementation("org.glassfish.jersey.media:jersey-media-multipart:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.core:jersey-client:${libs.versions.jersey.get()}")
    implementation("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:${libs.versions.jackson.get()}")
    implementation("jakarta.xml.bind:jakarta.xml.bind-api:${libs.versions.jakarta.xml.bind.get()}")
    implementation("jakarta.activation:jakarta.activation-api:${libs.versions.jakarta.activation.get()}")
    runtimeOnly("com.sun.activation:jakarta.activation:${libs.versions.jakarta.activation.get()}")
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.commons.lang3)
    implementation(libs.completable.futures)
    implementation(libs.netty.codec.http)
    implementation(libs.netty.handler)
    implementation(libs.netty.common)
    implementation(libs.slf4j.api)
    implementation(project(":pulsar-package-management:core"))

    // Test
    testImplementation(libs.hamcrest)
    testImplementation(libs.awaitility)
    testImplementation(libs.wiremock)
    testImplementation(libs.opentelemetry.api)
    testImplementation(project(":buildtools"))
}
