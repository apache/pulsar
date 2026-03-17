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
    id("pulsar.test-jar")
    id("pulsar.lightproto")
}

description = "Pulsar Transaction Coordinator"

dependencies {
    api(project(":pulsar-transaction:common"))
    api(project(":managed-ledger"))
    api(project(":pulsar-broker-common"))
    api(project(":pulsar-opentelemetry"))
    implementation(libs.slf4j.api)
    implementation(libs.commons.lang3)
    implementation(libs.guava)
    implementation(libs.netty.buffer)
    implementation(libs.netty.common)
    implementation(libs.simpleclient)
    implementation(libs.opentelemetry.api)
    implementation(libs.jctools.core)
    implementation(libs.commons.collections4)
    implementation(libs.bookkeeper.server) {
        exclude(group = "org.bouncycastle")
        exclude(group = "io.netty")
        exclude(group = "org.apache.zookeeper")
    }

    testImplementation(project(":testmocks"))
    testImplementation(libs.awaitility)
    testImplementation(project(":buildtools"))
}
