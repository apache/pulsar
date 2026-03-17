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
    id("com.google.protobuf")
}

description = "Pulsar Functions Proto"

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf3.get()}"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

dependencies {
    api(libs.protobuf.java)
    api(libs.grpc.stub)
    api(libs.grpc.protobuf)
    api(libs.grpc.api)
    api(libs.jakarta.annotation.api)
    implementation("com.google.protobuf:protobuf-java-util:${libs.versions.protobuf3.get()}")
    runtimeOnly(libs.perfmark.api)
    testImplementation(project(":buildtools"))
}
