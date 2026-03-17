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

description = "Pulsar Broker Auth - OIDC"

dependencies {
    api(project(":pulsar-broker-common"))
    implementation("com.auth0:java-jwt:4.3.0")
    implementation("com.auth0:jwks-rsa:0.22.0")
    implementation(libs.nimbus.jose.jwt)
    implementation(libs.caffeine)
    implementation(libs.async.http.client) {
        exclude(group = "io.netty")
        exclude(group = "com.typesafe.netty")
    }
    implementation(libs.kubernetes.client) {
        exclude(group = "io.prometheus", module = "simpleclient_httpserver")
        exclude(group = "org.bouncycastle")
        exclude(group = "javax.annotation", module = "javax.annotation-api")
    }
    implementation(libs.okhttp3)
    implementation(libs.jackson.databind)
    implementation(libs.commons.lang3)
    implementation(libs.netty.handler)
    implementation(libs.opentelemetry.api)
    implementation(libs.simpleclient.caffeine)
    implementation(libs.guava)
    implementation(libs.slf4j.api)

    testImplementation(project(":buildtools"))
    testImplementation(project(":testmocks"))
    testImplementation(libs.wiremock)
    testImplementation(libs.awaitility)
    testImplementation(project(path = ":pulsar-broker", configuration = "testArtifacts"))
    testImplementation(libs.jjwt.impl)
    testImplementation(libs.jjwt.jackson)
}
