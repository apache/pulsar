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

// Use the shadow JAR which contains relocated netty/jackson/etc classes.
// In Maven, pulsar-client-admin-shaded produces the "pulsar-client-admin" artifact,
// so the Maven shade test depends on the shaded JAR, not the original.

dependencies {
    testImplementation(project(":pulsar-client-admin-shaded"))
    // API modules and messagecrypto are not bundled in the shaded JAR
    testImplementation(project(":pulsar-client-admin-api"))
    testImplementation(project(":pulsar-client-messagecrypto-bc"))
    testImplementation(project(":buildtools"))
    testImplementation(libs.bcprov.jdk18on)
    testImplementation(libs.testcontainers)
    // Runtime deps needed by the client that are not bundled in the shaded JARs.
    // The admin shaded JAR includes JSONSchema (from pulsar-client) which references
    // Avro, but unlike the client shaded JAR, admin doesn't bundle/relocate Avro.
    testRuntimeOnly(libs.opentelemetry.api)
    testRuntimeOnly(libs.opentelemetry.api.incubator)
    testRuntimeOnly(libs.avro)
}

tasks.named<Test>("test") {
    useTestNG {
        suiteXmlFiles = listOf(file("src/test/resources/pulsar.xml"))
    }
}
