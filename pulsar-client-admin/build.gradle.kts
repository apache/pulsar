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
    id("pulsar.public-java-library-conventions")
}

dependencies {
    implementation(libs.slog)
    api(project(":pulsar-client-admin-api"))
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-common"))
    implementation(project(":pulsar-package-management:pulsar-package-core"))
    implementation(libs.jersey.client)
    implementation(libs.jersey.media.json.jackson)
    implementation(libs.jersey.media.multipart)
    implementation(libs.jersey.hk2)
    implementation(libs.jackson.jaxrs.json.provider)
    implementation(libs.jackson.databind)
    implementation(libs.jakarta.ws.rs.api)
    implementation(libs.jakarta.xml.bind.api)
    implementation(libs.jakarta.activation.api)
    runtimeOnly(libs.jakarta.activation)
    implementation(libs.guava)
    implementation(libs.gson)
    implementation(libs.asynchttpclient)
    implementation(libs.commons.lang3)
    implementation(libs.completable.futures)

    testImplementation(libs.wiremock)
}
