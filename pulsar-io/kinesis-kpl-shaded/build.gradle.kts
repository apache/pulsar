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
    id("pulsar.shade-conventions")
}

description = "Pulsar IO :: Kinesis KPL Shaded"

dependencies {
    implementation("software.amazon.kinesis:amazon-kinesis-producer:1.0.4")
    implementation("com.google.protobuf:protobuf-java:4.29.0")
}

tasks.shadowJar {
    relocate("com.google.protobuf", "org.apache.pulsar.io.kinesis.shaded.com.google.protobuf")

    dependencies {
        include(dependency("software.amazon.kinesis:amazon-kinesis-producer"))
        include(dependency("com.google.protobuf:protobuf-java"))
    }
}
