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

// Use the shadow JARs which contain relocated netty/jackson/etc classes.
// In Maven, pulsar-client-shaded produces the "pulsar-client" artifact and
// pulsar-client-admin-shaded produces the "pulsar-client-admin" artifact,
// so the Maven shade tests depend on the shaded JARs, not the originals.

dependencies {
    testImplementation(project(":pulsar-client-shaded"))
    testImplementation(project(":pulsar-client-admin-shaded"))
    // API modules are not bundled in the shaded JARs
    testImplementation(project(":pulsar-client-api"))
    testImplementation(project(":pulsar-client-admin-api"))
    testImplementation(project(":buildtools"))
    testImplementation(libs.bcprov.jdk18on)
    testImplementation(libs.bcpkix.jdk18on)
    testImplementation(libs.testcontainers)
    // Runtime deps needed by the client that are not bundled in the shaded JARs
    testRuntimeOnly(libs.opentelemetry.api)
    testRuntimeOnly(libs.opentelemetry.api.incubator)
}

tasks.named<Test>("test") {
    useTestNG {
        suiteXmlFiles = listOf(file("src/test/resources/pulsar.xml"))
    }
}
