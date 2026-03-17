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

description = "Pulsar Metadata"

tasks.withType<Test>().configureEach {
    maxParallelForks = (project.findProperty("testForkCount") as? String)?.toInt() ?: 2
}

dependencies {
    api(project(":pulsar-common"))
    implementation(project(":jetcd-core-shaded")) {
        exclude(group = "io.etcd")
        exclude(group = "io.vertx")
    }

    implementation(libs.zookeeper) {
        exclude(group = "ch.qos.logback")
        exclude(group = "io.netty", module = "netty-tcnative")
    }

    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.stub)
    implementation(libs.caffeine)
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.caffeine)
    implementation(libs.opentelemetry.api)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.oxia.client)
    implementation(libs.bookkeeper.server) {
        exclude(group = "org.bouncycastle")
        exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "org.jboss.netty", module = "netty")
        exclude(group = "io.netty")
        exclude(group = "org.apache.zookeeper", module = "zookeeper")
    }

    testImplementation(project(":testmocks"))
    testImplementation("org.apache.bookkeeper:bookkeeper-common:${libs.versions.bookkeeper.get()}:tests")
    testImplementation("org.apache.bookkeeper:bookkeeper-server:${libs.versions.bookkeeper.get()}:tests")
    testImplementation(libs.oxia.testcontainers)
    testImplementation(libs.jetcd.test) {
        exclude(group = "io.grpc", module = "grpc-netty")
        exclude(group = "io.etcd", module = "jetcd-core")
        exclude(group = "io.etcd", module = "jetcd-api")
        exclude(group = "io.vertx")
    }
    testImplementation(libs.dropwizard.metrics.core)
    testImplementation(libs.zookeeper) {
        artifact { classifier = "tests" }
        exclude(group = "ch.qos.logback")
        exclude(group = "io.netty", module = "netty-tcnative")
    }
    testImplementation(libs.snappy.java)
    testImplementation(libs.awaitility)
    testImplementation(project(":buildtools"))
}

tasks.jar {
    manifest {
        attributes("Automatic-Module-Name" to "org.apache.pulsar.metadata")
    }
}
