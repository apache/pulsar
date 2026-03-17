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

description = "Pulsar IO :: Debezium :: Core"

dependencies {
    compileOnly(project(":pulsar-io-core"))
    implementation(libs.debezium.core)
    implementation(libs.commons.lang3)
    implementation(libs.guava)
    implementation(project(":pulsar-common")) {
        exclude(group = "io.prometheus", module = "simpleclient_caffeine")
    }
    api(project(":pulsar-io:kafka-connect-adaptor"))
    api("org.apache.kafka:connect-runtime:${libs.versions.kafka.client.get()}") {
        exclude(group = "org.apache.kafka", module = "kafka-log4j-appender")
        exclude(group = "org.bitbucket.b_c", module = "jose4j")
        exclude(group = "org.eclipse.jetty")
        exclude(group = "org.lz4", module = "lz4-java")
    }

    testImplementation(project(":buildtools"))
    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testArtifacts"))
    testImplementation(project(path = ":managed-ledger", configuration = "testArtifacts"))
    testImplementation(project(":testmocks"))
    testImplementation("org.apache.bookkeeper:bookkeeper-common:4.17.3:tests")
    testImplementation("org.apache.bookkeeper:bookkeeper-server:4.17.3:tests")
    testImplementation(project(":pulsar-broker"))
    testImplementation(project(":testmocks"))
    testImplementation(libs.debezium.connector.mysql)
    testImplementation(project(":pulsar-client"))
}
