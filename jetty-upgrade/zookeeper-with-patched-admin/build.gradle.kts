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
    id("pulsar.shadow-conventions")
}

dependencies {
    implementation(libs.zookeeper)
    implementation(project(":jetty-upgrade:pulsar-zookeeper-prometheus-metrics"))
    implementation(libs.jetty.server)
    implementation(libs.jetty.ee8.servlet)
    compileOnly(libs.jackson.databind)
    compileOnly(libs.spotbugs.annotations)
}

tasks.shadowJar {
    // Only merge zookeeper dependency into the shadow jar (matching Maven shade artifactSet).
    // The module's own classes (patched admin replacements) are included by default.
    dependencies {
        include(dependency("org.apache.zookeeper:zookeeper"))
    }
}

