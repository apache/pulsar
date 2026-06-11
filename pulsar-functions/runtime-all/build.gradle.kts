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
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":pulsar-io:pulsar-io-core"))
    implementation(project(":pulsar-functions:pulsar-functions-api"))
    implementation(project(":pulsar-client-api"))
    implementation(libs.avro)
    implementation(libs.jackson.databind)
    implementation(libs.protobuf.java)
    implementation(libs.gson)
    implementation(libs.slf4j.api)
    implementation(libs.log4j.slf4j2.impl)
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
}

// Build a fat JAR as java-instance.jar using the Shadow plugin.
// Shadow handles dependency resolution lazily, avoiding configuration cache invalidation
// that occurred with the previous manual dependsOn(runtimeClasspath) + zipTree() approach.
tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveFileName.set("java-instance.jar")
    mergeServiceFiles()
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

// Consumable configuration exposing the shadow jar for cross-project dependencies.
// Unlike pulsar.shadow-conventions (which replaces runtimeElements), this project
// uses the Shadow plugin directly, so we create a dedicated configuration.
val shadowJarElements by configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    outgoing {
        artifact(tasks.named("shadowJar"))
    }
}

tasks.withType<Test>().configureEach {
    dependsOn(tasks.named("shadowJar"))
}
