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

// Published under the historical Maven artifactId "pulsar-client" (the shaded client). The Gradle
// project is ":pulsar-client-shaded" because the "pulsar-client" directory holds pulsar-client-original.
// archivesName drives the shaded jar base name, the published artifactId, and this project's module
// identity (so the BOM and NAR exclusions reference "pulsar-client").
base {
    archivesName.set("pulsar-client")
}

dependencies {
    // Bundled into the shaded jar (kept on the runtime classpath so shadowJar packs them):
    implementation(project(":pulsar-client-original")) {
        // fastutil is bundled via :pulsar-client-fastutil-minimized, which packs only the
        // (unrelocated) fastutil classes actually used instead of the full ~25MB jar.
        exclude(group = "it.unimi.dsi", module = "fastutil")
    }
    implementation(project(":pulsar-client-fastutil-minimized"))
    implementation(project(":pulsar-client-messagecrypto-bc"))

    // Non-bundled dependencies. These are the ONLY entries in the dependency-reduced published
    // POM/Gradle Module Metadata. The bundled modules above and their relocated third-party deps
    // live inside the jar and must NOT appear as POM dependencies. pulsar-client-api,
    // jackson-annotations, the BouncyCastle jars (signed, not relocatable), OpenTelemetry and
    // jspecify are deliberately not bundled. Logging is slf4j + slog only — never log4j.
    //
    // `shadowApi` -> compile scope, `shadow` -> runtime scope (see pulsar.client-shade-conventions).
    // The split follows what the SHADED artifact's public API exposes vs. what is only used
    // internally by the bundled code:
    //  - compile: types reachable from the public API a consumer compiles against —
    //    pulsar-client-api and io.opentelemetry:opentelemetry-api (ClientBuilder.openTelemetry / *Stats).
    //  - runtime: only used internally by the bundled implementation (BouncyCastle crypto, relocated
    //    Jackson's unshaded annotations, the OpenTelemetry incubator, slog/slf4j logging, jspecify
    //    CLASS-retention annotations) — never on a consumer's compile classpath.
    //
    // protobuf-java is intentionally NOT published (neither bundled nor declared as a dependency):
    // the generated protobuf classes reference unshaded com.google.protobuf at runtime, but Pulsar
    // does not force a protobuf version on consumers. Anyone using Schema.PROTOBUF / PROTOBUF_NATIVE
    // must add protobuf-java themselves — matching the `provided` scope of the pre-Gradle Maven build.
    "shadowApi"(project(":pulsar-client-api"))
    "shadowApi"(libs.opentelemetry.api)
    "shadow"(libs.jackson.annotations)
    "shadow"(libs.bcprov.jdk18on)
    "shadow"(libs.bcpkix.jdk18on)
    "shadow"(libs.opentelemetry.api.incubator)
    "shadow"(libs.slog)
    "shadow"(libs.slf4j.api)
    "shadow"(libs.jspecify)
}
