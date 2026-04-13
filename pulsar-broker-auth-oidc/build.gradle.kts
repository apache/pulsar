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
    implementation(project(":pulsar-broker-common"))
    implementation(libs.auth0.java.jwt)
    implementation(libs.auth0.jwks.rsa)
    implementation(libs.caffeine)
    implementation(libs.asynchttpclient)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.annotations)
    implementation(libs.kubernetes.client.java)
    implementation(libs.okhttp3)
    implementation(libs.commons.lang3)
    implementation(libs.opentelemetry.api)
    implementation(libs.simpleclient.caffeine)

    testImplementation(libs.wiremock)
    testImplementation(libs.jjwt.api)
    testImplementation(libs.jjwt.impl)
    testImplementation(libs.jjwt.jackson)
}

tasks.withType<Test> {
    environment("KUBECONFIG_TEMPLATE", "src/test/java/resources/fakeKubeConfig.yaml")
    environment("KUBECONFIG", layout.buildDirectory.file("kubeconfig.yaml").get().asFile.absolutePath)
}
