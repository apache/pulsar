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

description = "Pulsar Test Mocks"

dependencies {
    api(project(":pulsar-metadata"))
    api(libs.zookeeper) {
        exclude(group = "ch.qos.logback")
        exclude(group = "io.netty", module = "netty-tcnative")
    }
    api(libs.zookeeper) {
        artifact { classifier = "tests" }
        exclude(group = "ch.qos.logback")
        exclude(group = "io.netty", module = "netty-tcnative")
    }
    api(libs.bookkeeper.server) {
        exclude(group = "org.bouncycastle")
        exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "org.jboss.netty", module = "netty")
        exclude(group = "io.netty")
        exclude(group = "org.apache.zookeeper", module = "zookeeper")
    }
    api(libs.snappy.java)
    api(libs.dropwizard.metrics.core)
    api(libs.mockito.core)
    api(libs.testng)
    api(libs.commons.lang3)
    api("org.objenesis:objenesis:${libs.versions.objenesis.get()}")

    implementation(project(":buildtools"))
}
