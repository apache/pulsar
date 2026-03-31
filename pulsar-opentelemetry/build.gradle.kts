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

dependencies {
    api(libs.opentelemetry.api)
    implementation(libs.opentelemetry.exporter.otlp)
    implementation(libs.opentelemetry.exporter.prometheus)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.sdk.extension.autoconfigure)
    implementation(libs.opentelemetry.instrumentation.resources)
    implementation(libs.opentelemetry.semconv)
    implementation(libs.opentelemetry.instrumentation.runtime.telemetry.java17)
    implementation(libs.guava)
    implementation(libs.commons.lang3)

    testImplementation(project(":testmocks"))
    testImplementation(project(path = ":pulsar-broker-common", configuration = "testJar"))
    testImplementation(libs.restassured)
    testImplementation(libs.awaitility)
    testImplementation(libs.opentelemetry.sdk.testing)
}

tasks.withType<Test> {
    systemProperty("otel.semconv-stability.opt-in", "jvm")
}
