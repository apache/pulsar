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

description = "Pulsar IO :: Jdbc :: Core"

dependencies {
    implementation(project(":pulsar-io:common"))
    implementation(project(":pulsar-io-core"))
    implementation(project(":pulsar-client"))
    implementation(libs.avro)
    implementation(libs.jackson.databind)
    implementation(libs.guava)
    implementation(libs.jackson.dataformat.yaml)
    compileOnly(libs.protobuf.java)

    testImplementation(project(":buildtools"))
    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testArtifacts"))
    testImplementation(project(path = ":managed-ledger", configuration = "testArtifacts"))
    testImplementation(project(":testmocks"))
    testImplementation("org.apache.bookkeeper:bookkeeper-common:4.17.3:tests")
    testImplementation("org.apache.bookkeeper:bookkeeper-server:4.17.3:tests")
}
