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
    id("pulsar.public-java-library-conventions")
    alias(libs.plugins.lightproto)
}

// Suppress warnings in LightProto generated code
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-dep-ann")
}

dependencies {
    // LightProto-generated message types expose io.netty.buffer.ByteBuf in their public API
    // (e.g. parseFrom/writeTo), so netty-buffer is an api dependency for consumers of this module.
    api(libs.netty.buffer)
    // The lightproto-generated gRPC stub only needs grpc-stub (+ grpc-api transitively); it uses its
    // own LightProto marshaller, so grpc-protobuf is not required. The Netty transport is pulled in by
    // the modules that actually open channels/servers (instance, runtime, localrun).
    api(libs.grpc.stub)
}

lightproto {
    generateJson = true
}