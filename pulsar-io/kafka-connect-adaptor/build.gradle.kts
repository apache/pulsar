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

description = "Pulsar IO :: Kafka Connect Adaptor"

dependencies {
    compileOnly(project(":pulsar-io-core"))
    implementation(project(":pulsar-common")) {
        exclude(group = "io.prometheus", module = "simpleclient_caffeine")
    }
    implementation(project(":pulsar-functions:utils"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation("org.apache.kafka:connect-runtime:${libs.versions.kafka.client.get()}") {
        exclude(group = "org.apache.kafka", module = "kafka-log4j-appender")
        exclude(group = "org.slf4")
        exclude(group = "org.eclipse.jetty")
        exclude(group = "org.bitbucket.b_c", module = "jose4j")
        exclude(group = "org.lz4", module = "lz4-java")
    }
    implementation("org.apache.kafka:connect-json:${libs.versions.kafka.client.get()}") {
        exclude(group = "org.bitbucket.b_c", module = "jose4j")
    }
    implementation("org.apache.kafka:connect-api:${libs.versions.kafka.client.get()}") {
        exclude(group = "org.bitbucket.b_c", module = "jose4j")
    }
    implementation("org.apache.kafka:connect-transforms:${libs.versions.kafka.client.get()}") {
        exclude(group = "org.bitbucket.b_c", module = "jose4j")
    }
    implementation(project(":pulsar-client")) {
        exclude(group = "*", module = "*")
    }
    implementation(libs.netty.buffer)
    implementation(libs.commons.lang3)
    implementation("io.confluent:kafka-connect-avro-converter:${libs.versions.confluent.get()}") {
        exclude(group = "org.apache.avro", module = "avro")
    }
    compileOnly(libs.avro)
    compileOnly(libs.protobuf.java)

    testImplementation(project(":buildtools"))
    testImplementation(project(":pulsar-broker"))
    testImplementation("org.apache.kafka:connect-file:${libs.versions.kafka.client.get()}") {
        exclude(group = "org.bitbucket.b_c", module = "jose4j")
    }
    testImplementation(project(":testmocks"))
    testImplementation(libs.awaitility)
    testImplementation(project(path = ":pulsar-broker", configuration = "testArtifacts"))
    testImplementation(libs.async.http.client)
    testImplementation(libs.testcontainers)
    testRuntimeOnly("org.eclipse.jetty:jetty-util:9.4.58.v20250814")
    testImplementation(libs.bc.fips)
    testImplementation(libs.netty.reactive.streams)
}
