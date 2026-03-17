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
    id("pulsar.shade-conventions")
}

description = "JEtcd Core Shaded"

dependencies {
    implementation(libs.jetcd.core) {
        exclude(group = "io.grpc", module = "grpc-netty")
    }
    implementation(libs.grpc.netty.shaded)
    implementation(libs.failsafe)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.util)
}

tasks.shadowJar {
    // Only shade io.vertx and netty-within-grpc to avoid conflicts
    dependencies {
        include(dependency("io.etcd:.*"))
        include(dependency("io.vertx:.*"))
    }

    relocate("io.vertx", "org.apache.pulsar.jetcd.shaded.io.vertx")
    relocate("io.grpc.netty", "io.grpc.netty.shaded.io.grpc.netty")
    relocate("io.netty", "io.grpc.netty.shaded.io.netty")
}
