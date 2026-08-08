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
    api(project(":pulsar-client-admin-api"))
    api(project(":pulsar-client-original"))
    api(project(":pulsar-common"))
    // PIP-478: AsyncHttpConnector (internal) uses the TLS factory SPI directly (PulsarTlsFactory / TlsHandle /
    // TlsPurpose) but never surfaces it on this module's exported ABI, so it is `implementation`. The HTTP SPI
    // is not named directly here (only pulsar-client's FrameworkHttpClientFactory is referenced), so no direct
    // http-client-api dependency is needed.
    implementation(project(":pulsar-tls-factory-api"))
    implementation(project(":pulsar-package-management:pulsar-package-core"))
    api(libs.jersey.client)
    implementation(libs.jersey.media.json.jackson)
    implementation(libs.jersey.media.multipart)
    implementation(libs.jersey.hk2)
    implementation(libs.jackson.jakarta.rs.json.provider)
    api(libs.jackson.databind)
    api(libs.jakarta.ws.rs.api)
    implementation(libs.jakarta.xml.bind.api)
    implementation(libs.jakarta.activation.api)
    runtimeOnly(libs.angus.activation)
    implementation(libs.guava)
    api(libs.gson)
    api(libs.asynchttpclient)
    implementation(libs.commons.lang3)
    implementation(libs.completable.futures)
    // PIP-478 stage 4b: the admin AsyncHttpConnector rides the PIP-478 TLS SPI on the new path, whose init
    // context carries an OpenTelemetry root (compile-only; the real root is supplied at runtime by the owning
    // component, matching pulsar-common / pulsar-tls-factory-api).
    compileOnly(libs.opentelemetry.api)

    testImplementation(libs.wiremock)
    testImplementation(libs.opentelemetry.api)
}
