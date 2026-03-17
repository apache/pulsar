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
    id("pulsar.nar")
}

description = "Pulsar IO :: Alluxio"

dependencies {
    implementation(project(":pulsar-io-core"))
    implementation("org.alluxio:alluxio-core-client-fs:2.9.4") {
        exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    }
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.guava)

    testImplementation(project(":buildtools"))
    testImplementation(project(":pulsar-client"))
    testImplementation("org.alluxio:alluxio-minicluster:2.9.4") {
        exclude(group = "org.glassfish", module = "javax.el")
        exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    }
}

// Alluxio embedded tests require specific platform support
tasks.test {
    onlyIf { project.hasProperty("integrationTests") }
}
