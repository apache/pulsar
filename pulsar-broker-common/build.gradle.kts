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
    id("pulsar.test-certs-conventions")
}

dependencies {
    api(project(":pulsar-metadata"))
    implementation(libs.guava)
    implementation(libs.commons.lang3)
    implementation(libs.bookkeeper.server)
    implementation(libs.opentelemetry.api)
    implementation(libs.simpleclient)
    implementation(libs.caffeine)
    implementation(libs.javax.servlet.api)
    implementation(libs.jakarta.ws.rs.api)
    implementation(libs.jjwt.impl)
    implementation(libs.jjwt.jackson)
    implementation(libs.jetty.server)
    implementation(libs.jetty.compression.server)
    implementation(libs.jetty.compression.gzip)
    implementation(libs.jetty.ee8.servlet)

    testImplementation(libs.bc.fips)
    testImplementation(libs.awaitility)
    testImplementation(libs.restassured)
    testImplementation(libs.jersey.server)
}
