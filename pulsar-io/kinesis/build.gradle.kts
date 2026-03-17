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
    id("pulsar.nar")
}

description = "Pulsar IO :: Kinesis"

dependencies {
    implementation(project(":pulsar-io:kinesis-kpl-shaded"))
    implementation("software.amazon.kinesis:amazon-kinesis-producer:1.0.4")
    implementation(project(":pulsar-io:common"))
    implementation(project(":pulsar-io-core"))
    implementation(project(":pulsar-io:aws"))
    implementation(libs.commons.lang3)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor")
    implementation(libs.avro)
    implementation("com.github.wnameless.json:json-flattener:0.13.0")
    implementation(libs.gson)
    implementation("com.amazonaws:aws-java-sdk-core:${libs.versions.aws.sdk.get()}")
    implementation("software.amazon.awssdk:utils:${libs.versions.aws.sdk2.get()}")
    implementation("software.amazon.kinesis:amazon-kinesis-client:3.1.2")
    implementation("com.google.flatbuffers:flatbuffers-java:1.9.0")
    implementation("javax.xml.bind:jaxb-api:2.3.0")

    testImplementation(project(":buildtools"))
    testImplementation(project(":pulsar-functions:instance"))
    testImplementation("org.testcontainers:localstack:${libs.versions.testcontainers.get()}")
    testImplementation(libs.awaitility)
    testImplementation(libs.jsonassert)
}

// Ensure NAR task waits for kinesis-kpl-shaded shadow JAR
tasks.named("nar") {
    dependsOn(project(":pulsar-io:kinesis-kpl-shaded").tasks.named("shadowJar"))
}
