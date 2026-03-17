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

description = "Pulsar Package Management - BookKeeper Storage"

dependencies {
    api(project(":pulsar-package-management:core"))
    implementation(libs.bookkeeper.server) {
        exclude(group = "org.bouncycastle")
        exclude(group = "io.netty")
    }
    implementation("org.apache.distributedlog:distributedlog-core:${libs.versions.bookkeeper.get()}") {
        exclude(group = "org.bouncycastle")
        exclude(group = "io.netty")
    }
    implementation(libs.guava)
    implementation(libs.netty.buffer)
    implementation(libs.slf4j.api)

    testImplementation(project(":buildtools"))
    testImplementation(project(":testmocks"))
    testImplementation(project(":managed-ledger"))
    testImplementation("org.apache.bookkeeper:bookkeeper-common:${libs.versions.bookkeeper.get()}:tests")
    testImplementation("org.apache.bookkeeper:bookkeeper-server:${libs.versions.bookkeeper.get()}:tests")
}
