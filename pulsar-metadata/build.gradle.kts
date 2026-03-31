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
    api(project(":pulsar-common"))
    implementation(libs.bookkeeper.server)
    implementation(libs.zookeeper) {
        exclude(group = "org.slf4j")
    }
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.oxia.client)
    implementation(libs.caffeine)
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.caffeine)

    testImplementation(project(":buildtools"))
    testImplementation(project(":testmocks"))
    testImplementation(project(path = ":managed-ledger", configuration = "testJar"))
    testImplementation(libs.bookkeeper.common) { artifact { classifier = "tests" } }
    testImplementation(libs.zookeeper) { artifact { classifier = "tests" } }
    testImplementation(libs.oxia.testcontainers)
    testImplementation(libs.dropwizardmetrics.core)
    testImplementation(libs.snappy.java)
    testImplementation(libs.awaitility)
}
