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

description = "Pulsar IO :: Kafka"

dependencies {
    implementation(project(":pulsar-io:common"))
    compileOnly(project(":pulsar-io-core"))
    implementation(project(":pulsar-client"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.guava)
    implementation(libs.kafka.client) {
        exclude(group = "org.bitbucket.b_c", module = "jose4j")
        exclude(group = "org.lz4", module = "lz4-java")
    }
    implementation("io.confluent:kafka-schema-registry-client:${libs.versions.confluent.get()}")
    implementation("io.confluent:kafka-avro-serializer:${libs.versions.confluent.get()}")
    implementation(libs.jjwt.impl)
    implementation(libs.jjwt.jackson)

    testImplementation(project(":buildtools"))
    testImplementation(libs.hamcrest)
    testImplementation(libs.awaitility)
    testImplementation(libs.bcprov.jdk18on)
}
