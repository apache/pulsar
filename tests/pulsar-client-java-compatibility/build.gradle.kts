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

// Use only the consumer dependency graph: broker fixtures and Functions implementations must
// never be needed to compile a Java 17 client or a user's Function/Source/Sink implementation.
dependencies {
    implementation(project(":pulsar-client-tools"))
    implementation(project(":pulsar-client-v5"))
    implementation(project(":pulsar-client-admin-original"))
    implementation(project(":pulsar-client-auth-athenz"))
    implementation(project(":pulsar-client-auth-sasl"))
    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(project(":pulsar-functions:pulsar-functions-api"))
    implementation(project(":pulsar-io:pulsar-io-core"))
}

// pulsar.java-conventions compiles/runs these tests with pulsarClientJavaVersion (default 17),
// independently of the build JVM or -PtestJavaVersion used for server tests.
