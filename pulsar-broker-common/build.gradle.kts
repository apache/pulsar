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
}

description = "Pulsar Broker Common"

dependencies {
    api(project(":pulsar-metadata"))

    implementation(libs.guava)
    implementation(libs.opentelemetry.api)
    implementation(libs.javax.servlet.api)
    implementation(libs.jakarta.ws.rs.api)
    implementation(libs.jjwt.impl)
    implementation(libs.jjwt.jackson)

    // Commons
    implementation(libs.commons.lang3)
    implementation(libs.commons.codec)
    implementation(libs.commons.io)
    implementation(libs.commons.collections4)
    implementation(libs.commons.configuration2)

    // BookKeeper
    implementation(libs.bookkeeper.server) {
        exclude(group = "org.bouncycastle")
        exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j")
        exclude(group = "org.jboss.netty")
        exclude(group = "io.netty")
        exclude(group = "org.apache.zookeeper")
    }
    implementation(libs.bookkeeper.common)

    // Zookeeper
    implementation(libs.zookeeper) {
        exclude(group = "ch.qos.logback")
        exclude(group = "io.netty", module = "netty-tcnative")
    }

    // Metrics
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)

    // Caching
    implementation(libs.caffeine)

    // Netty (needed for buffer/util refs)
    implementation(libs.netty.buffer)
    implementation(libs.netty.common)

    // Jetty
    implementation(libs.jetty.server)
    implementation(libs.jetty.compression.server)
    implementation(libs.jetty.compression.gzip)
    implementation(libs.jetty.ee8.servlet)

    // Logging
    implementation(libs.slf4j.api)

    // Test
    testImplementation(libs.bc.fips)
    testImplementation(libs.awaitility)
    testImplementation(libs.restassured)
    testImplementation(libs.jersey.server)
    testImplementation(project(":buildtools"))
}
