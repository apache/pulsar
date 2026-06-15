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
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-client-messagecrypto-bc"))

    // Non-bundled runtime dependencies. These are the ONLY entries in the dependency-reduced
    // published POM/Gradle Module Metadata (the `shadow` component). The bundled modules above and
    // their relocated third-party deps live inside the jar and must NOT appear as POM dependencies.
    // pulsar-client-api, protobuf, jackson-annotations, the BouncyCastle jars (signed, not
    // relocatable), OpenTelemetry and jspecify are deliberately not bundled. Logging is slf4j + slog
    // only — never log4j.
    "shadow"(project(":pulsar-client-api"))
    "shadow"(libs.protobuf.java)
    "shadow"(libs.jackson.annotations)
    "shadow"(libs.bcprov.jdk18on)
    "shadow"(libs.bcpkix.jdk18on)
    "shadow"(libs.opentelemetry.api)
    "shadow"(libs.opentelemetry.api.incubator)
    "shadow"(libs.jspecify)
    "shadow"(libs.slf4j.api)
    "shadow"(libs.slog)
}
