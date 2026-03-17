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

description = "Pulsar Functions Local Runner (Shaded)"

dependencies {
    implementation(project(":pulsar-functions:localrun"))
}

tasks.shadowJar {
    // Extensive relocations for the local runner
    relocate("com.fasterxml.jackson", "org.apache.pulsar.shade.com.fasterxml.jackson")
    relocate("com.google", "org.apache.pulsar.shade.com.google")
    relocate("com.spotify.futures", "org.apache.pulsar.shade.com.spotify.futures")
    relocate("com.typesafe", "org.apache.pulsar.shade.com.typesafe")
    relocate("io.airlift", "org.apache.pulsar.shade.io.airlift")
    relocate("io.netty", "org.apache.pulsar.shade.io.netty")
    relocate("io.swagger", "org.apache.pulsar.shade.io.swagger")
    relocate("io.prometheus.client", "org.apache.pulsar.shade.io.prometheus.client")
    relocate("javax.activation", "org.apache.pulsar.shade.javax.activation")
    relocate("javax.annotation", "org.apache.pulsar.shade.javax.annotation")
    relocate("javax.ws", "org.apache.pulsar.shade.javax.ws")
    relocate("org.apache.avro", "org.apache.pulsar.shade.org.apache.avro")
    relocate("org.apache.bookkeeper", "org.apache.pulsar.shade.org.apache.bookkeeper")
    relocate("org.apache.commons", "org.apache.pulsar.shade.org.apache.commons")
    relocate("org.apache.curator", "org.apache.pulsar.shade.org.apache.curator")
    relocate("org.apache.zookeeper", "org.apache.pulsar.shade.org.apache.zookeeper")
    relocate("org.asynchttpclient", "org.apache.pulsar.shade.org.asynchttpclient")
    relocate("org.checkerframework", "org.apache.pulsar.shade.org.checkerframework")
    relocate("org.eclipse.jetty", "org.apache.pulsar.shade.org.eclipse.jetty")
    relocate("org.glassfish", "org.apache.pulsar.shade.org.glassfish")
    relocate("org.objenesis", "org.apache.pulsar.shade.org.objenesis")
    relocate("org.reactivestreams", "org.apache.pulsar.shade.org.reactivestreams")
    relocate("org.yaml", "org.apache.pulsar.shade.org.yaml")

    dependencies {
        exclude(dependency("org.bouncycastle:.*"))
    }
}
