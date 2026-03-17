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

description = "Pulsar Functions Utils"

dependencies {
    api(project(":pulsar-functions:proto"))
    api(project(":pulsar-functions:api-java"))
    api(project(":pulsar-common"))
    api(project(":pulsar-client-api"))
    api(project(":pulsar-client"))
    api(project(":pulsar-io-core"))
    api(project(":pulsar-config-validation"))
    implementation(project(":pulsar-package-management:core"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.annotations)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.gson)
    implementation(libs.slf4j.api)
    implementation(libs.typetools)
    implementation(libs.byte.buddy)
    implementation(libs.zt.zip)
    implementation(libs.commons.lang3)
    implementation("com.google.protobuf:protobuf-java-util:${libs.versions.protobuf3.get()}")

    testImplementation(project(":buildtools"))
    testImplementation(libs.jsonassert)
    testImplementation(libs.wiremock)
}
