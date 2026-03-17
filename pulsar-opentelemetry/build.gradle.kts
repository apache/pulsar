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

description = "Pulsar OpenTelemetry"

val otelVersion = "1.56.0"
val otelInstrumentationVersion = "2.21.0"

dependencies {
    api(libs.opentelemetry.api)
    api(libs.opentelemetry.sdk)
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:$otelVersion")
    implementation("io.opentelemetry:opentelemetry-exporter-prometheus:$otelVersion-alpha")
    implementation("io.opentelemetry:opentelemetry-sdk-extension-autoconfigure:$otelVersion")
    implementation(libs.opentelemetry.semconv)
    implementation(libs.opentelemetry.instrumentation.resources)
    implementation("io.opentelemetry.instrumentation:opentelemetry-runtime-telemetry-java17:$otelInstrumentationVersion-alpha")
    implementation(libs.guava)
    implementation(libs.commons.lang3)

    testImplementation(project(path = ":pulsar-broker-common", configuration = "testArtifacts"))
    testImplementation(libs.restassured)
    testImplementation(libs.awaitility)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(project(":buildtools"))
}

tasks.test {
    // Required for OTel JVM runtime metrics with stable semconv names
    systemProperty("otel.semconv-stability.opt-in", "jvm")
}
