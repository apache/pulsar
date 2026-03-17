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

description = "Pulsar Functions Runtime"

dependencies {
    api(project(":pulsar-functions:instance"))
    api(project(":pulsar-functions:utils"))
    api(project(":pulsar-broker-common"))
    implementation(libs.kubernetes.client)
    implementation(libs.guava)
    implementation(libs.commons.lang3)
    implementation(libs.picocli)
    implementation(libs.jackson.dataformat.yaml)
    implementation("com.google.protobuf:protobuf-java-util:${libs.versions.protobuf3.get()}")
    implementation(libs.byte.buddy)
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.prometheus.jmx.collector)
    implementation("org.eclipse.jetty:jetty-util:${libs.versions.jetty.get()}")
    implementation(libs.slf4j.api)

    testImplementation(project(":buildtools"))
    testImplementation(libs.awaitility)
}
