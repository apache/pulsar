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

description = "Pulsar Client Auth - Athenz"

dependencies {
    api(project(":pulsar-client-api"))
    compileOnly(project(":pulsar-client"))
    implementation(libs.athenz.zts.java.client.core) {
        exclude(group = "javax.ws.rs", module = "javax.ws.rs-api")
    }
    implementation(libs.athenz.auth.core) {
        exclude(group = "org.bouncycastle")
    }
    implementation(libs.athenz.cert.refresher) {
        exclude(group = "org.bouncycastle")
    }
    implementation(libs.jakarta.ws.rs.api)
    implementation(libs.bcpkix.jdk18on)
    implementation(libs.guava)
    implementation(libs.commons.lang3)
    implementation(libs.slf4j.api)

    testImplementation(project(":buildtools"))
    testImplementation(project(":pulsar-common"))
    testImplementation(project(":pulsar-client"))
}
