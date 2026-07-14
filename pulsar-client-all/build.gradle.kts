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
    id("pulsar.client-shade-conventions")
}

dependencies {
    // Bundled into the shaded jar (kept on the runtime classpath so shadowJar packs them):
    implementation(project(":pulsar-client-original")) {
        // fastutil is bundled via :pulsar-client-fastutil-minimized, which packs only the
        // (unrelocated) fastutil classes actually used instead of the full ~25MB jar.
        exclude(group = "it.unimi.dsi", module = "fastutil")
    }
    implementation(project(":pulsar-client-fastutil-minimized"))
    implementation(project(":pulsar-client-admin-original"))
    implementation(project(":pulsar-client-messagecrypto-bc"))

    // Non-bundled dependencies for the dependency-reduced published POM/GMM — union of the client
    // and admin shaded sets. Logging is slf4j + slog only — never log4j (the log4j entries below are
    // test-only and never reach the publication). `shadowApi` -> compile scope, `shadow` -> runtime
    // scope (see pulsar.client-shade-conventions). compile = exposed in the public API a consumer
    // compiles against (the *-api modules, OpenTelemetry via ClientBuilder/Stats); runtime = used
    // only internally by the bundled code (BouncyCastle, the unshaded Jackson annotations, the OTel
    // incubator, slog/slf4j logging, jspecify CLASS-retention annotations).
    //
    // protobuf-java is intentionally NOT published (it was `provided` in the pre-Gradle Maven build):
    // consumers using Schema.PROTOBUF / PROTOBUF_NATIVE must add protobuf-java themselves.
    "shadowApi"(project(":pulsar-client-api"))
    "shadowApi"(project(":pulsar-client-admin-api"))
    "shadowApi"(libs.opentelemetry.api)
    "shadow"(libs.jackson.annotations)
    "shadow"(libs.bcprov.jdk18on)
    "shadow"(libs.bcpkix.jdk18on)
    "shadow"(libs.opentelemetry.api.incubator)
    "shadow"(libs.slog)
    "shadow"(libs.slf4j.api)
    "shadow"(libs.jspecify)

    testImplementation(libs.log4j.api)
    testImplementation(libs.log4j.core)
    testImplementation(libs.log4j.slf4j2.impl)
}
