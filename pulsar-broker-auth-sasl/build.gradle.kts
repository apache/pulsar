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

// Each test class gets its own JVM fork because AuthenticationSasl has static JAAS state
// (jaasCredentialsContainer, initializedJAAS) that leaks between test classes sharing a fork.
tasks.withType<Test> {
    forkEvery = 1
}

dependencies {
    implementation(libs.slog)
    implementation(project(":pulsar-broker"))
    implementation(project(":pulsar-broker-common"))
    implementation(project(":pulsar-common"))
    implementation(libs.guava)
    implementation(libs.caffeine)
    implementation(libs.commons.lang3)
    implementation(libs.commons.codec)
    implementation(libs.javax.servlet.api)
    implementation(libs.simpleclient.caffeine)

    testImplementation(libs.commons.io)
    testImplementation(libs.kerby.simplekdc)
    testImplementation(project(":pulsar-client-original"))
    testImplementation(project(":pulsar-proxy"))
    testImplementation(project(":pulsar-client-auth-sasl"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testJar"))
}
