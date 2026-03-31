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

dependencies {
    api(project(":pulsar-functions:pulsar-functions-instance"))
    api(project(":pulsar-functions:pulsar-functions-runtime"))
    implementation(libs.picocli)
    implementation(libs.gson)
    implementation(libs.commons.lang3)
    implementation(libs.simpleclient.httpserver)
    implementation(libs.jackson.databind)
    implementation(libs.grpc.all)
    implementation(project(":pulsar-functions:pulsar-functions-proto"))
    implementation(project(":pulsar-functions:pulsar-functions-secrets"))
    runtimeOnly(libs.perfmark.api)
    runtimeOnly(libs.log4j.slf4j2.impl)
    runtimeOnly(libs.log4j.api)
    runtimeOnly(libs.log4j.core)
}
