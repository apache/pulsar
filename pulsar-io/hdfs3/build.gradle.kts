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

description = "Pulsar IO :: Hdfs3"

dependencies {
    implementation(project(":pulsar-io-core"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.commons.collections4)
    implementation("org.apache.hadoop:hadoop-client:${libs.versions.hadoop3.get()}") {
        exclude(group = "jakarta.activation", module = "jakarta.activation-api")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "org.slf4j")
        exclude(group = "org.apache.avro", module = "avro")
        exclude(group = "org.bouncycastle", module = "bcprov-jdk15on")
    }
    implementation(libs.bcprov.jdk18on)
    implementation("jakarta.activation:jakarta.activation-api:${libs.versions.jakarta.activation.get()}")
    implementation(libs.slf4j.api)

    testImplementation(project(":buildtools"))
}
