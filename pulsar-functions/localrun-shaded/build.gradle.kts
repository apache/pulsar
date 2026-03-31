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
    implementation(project(":pulsar-functions:pulsar-functions-local-runner-original"))
}

val shadePrefix = "org.apache.pulsar.functions.runtime.shaded"

tasks.shadowJar {
    isZip64 = true

    dependencies {
        include(dependency("org.apache.pulsar:.*"))
        include(project(":pulsar-functions:pulsar-functions-local-runner-original"))
        include(project(":pulsar-client-original"))
        include(project(":pulsar-common"))
        include(project(":pulsar-client-admin-original"))
        include(dependency("org.apache.bookkeeper:.*"))
        include(dependency("commons-.*:.*"))
        include(dependency("org.apache.commons:.*"))
        include(dependency("com.fasterxml.jackson.*:.*"))
        include(dependency("io.netty:.*"))
        include(dependency("io.netty.incubator:.*"))
        include(dependency("com.google.*:.*"))
        include(dependency("javax.servlet:.*"))
        include(dependency("org.reactivestreams:reactive-streams"))
        include(dependency("io.swagger:.*"))
        include(dependency("org.yaml:snakeyaml"))
        include(dependency("io.perfmark:.*"))
        include(dependency("io.prometheus:.*"))
        include(dependency("io.prometheus.jmx:.*"))
        include(dependency("javax.ws.rs:.*"))
        include(dependency("org.tukaani:xz"))
        include(dependency("com.github.zafarkhaja:java-semver"))
        include(dependency("net.java.dev.jna:.*"))
        include(dependency("org.apache.zookeeper:.*"))
        include(dependency("com.thoughtworks.paranamer:paranamer"))
        include(dependency("jline:.*"))
        include(dependency("org.rocksdb:.*"))
        include(dependency("org.eclipse.jetty.*:.*"))
        include(dependency("org.apache.avro:avro"))
        include(dependency("info.picocli:.*"))
        include(dependency("net.jodah:.*"))
        include(dependency("io.airlift:.*"))
        include(dependency("com.yahoo.datasketches:.*"))
    }

    // Exclude bouncycastle from pulsar-client (signatures would break if shaded)
    exclude("org/bouncycastle/**")

    relocateWithPrefix(shadePrefix, "aj.org")
    relocateWithPrefix(shadePrefix, "avro.shaded")
    relocateWithPrefix(shadePrefix, "com.fasterxml")
    relocateWithPrefix(shadePrefix, "com.github")
    relocateWithPrefix(shadePrefix, "com.github.luben")
    relocateWithPrefix(shadePrefix, "com.google")
    relocateWithPrefix(shadePrefix, "com.scurrilous")
    relocateWithPrefix(shadePrefix, "com.squareup.okhttp")
    relocateWithPrefix(shadePrefix, "com.squareup.okio")
    relocateWithPrefix(shadePrefix, "com.thoughtworks.paranamer")
    relocateWithPrefix(shadePrefix, "com.yahoo.datasketches")
    relocateWithPrefix(shadePrefix, "com.yahoo.sketches")
    relocateWithPrefix(shadePrefix, "commons-cli")
    relocateWithPrefix(shadePrefix, "commons-codec")
    relocateWithPrefix(shadePrefix, "commons-io")
    relocateWithPrefix(shadePrefix, "commons-lang")
    relocateWithPrefix(shadePrefix, "dlshade")
    relocateWithPrefix(shadePrefix, "info.picocli")
    relocateWithPrefix(shadePrefix, "io.airlift")
    relocateWithPrefix(shadePrefix, "io.grpc")
    relocateWithPrefix(shadePrefix, "io.jsonwebtoken")
    relocateWithPrefix(shadePrefix, "io.kubernetes")
    relocateWithPrefix(shadePrefix, "io.netty")
    relocateWithPrefix(shadePrefix, "io.opencensus")
    relocateWithPrefix(shadePrefix, "io.perfmark")
    relocateWithPrefix(shadePrefix, "io.prometheus")
    relocateWithPrefix(shadePrefix, "io.swagger")
    relocateWithPrefix(shadePrefix, "javax.activation")
    relocateWithPrefix(shadePrefix, "javax.annotation")
    relocateWithPrefix(shadePrefix, "javax.servlet")
    relocateWithPrefix(shadePrefix, "javax.validation")
    relocateWithPrefix(shadePrefix, "javax.ws.rs")
    relocateWithPrefix(shadePrefix, "jline")
    relocateWithPrefix(shadePrefix, "net.java.dev.jna")
    relocateWithPrefix(shadePrefix, "net.jodah")
    relocateWithPrefix(shadePrefix, "net.jpountz")
    relocateWithPrefix(shadePrefix, "okio")
    relocateWithPrefix(shadePrefix, "org.apache.avro")
    relocateWithPrefix(shadePrefix, "org.apache.bookkeeper")
    relocateWithPrefix(shadePrefix, "org.apache.commons")
    relocateWithPrefix(shadePrefix, "org.apache.curator")
    relocateWithPrefix(shadePrefix, "org.apache.distributedlog")
    relocateWithPrefix(shadePrefix, "org.apache.jute")
    relocateWithPrefix(shadePrefix, "org.apache.yetus")
    relocateWithPrefix(shadePrefix, "org.apache.zookeeper")
    relocateWithPrefix(shadePrefix, "org.asynchttpclient")
    relocateWithPrefix(shadePrefix, "org.bookkeeper")
    relocateWithPrefix(shadePrefix, "org.checkerframework")
    relocateWithPrefix(shadePrefix, "org.codehaus.jackson")
    relocateWithPrefix(shadePrefix, "org.codehaus.mojo")
    relocateWithPrefix(shadePrefix, "org.eclipse.jetty")
    relocateWithPrefix(shadePrefix, "org.hamcrest")
    relocateWithPrefix(shadePrefix, "org.inferred")
    relocateWithPrefix(shadePrefix, "org.jctools")
    relocateWithPrefix(shadePrefix, "org.joda")
    relocateWithPrefix(shadePrefix, "org.lz4")
    relocateWithPrefix(shadePrefix, "org.objenesis")
    relocateWithPrefix(shadePrefix, "org.reactivestreams")
    relocateWithPrefix(shadePrefix, "org.rocksdb")
    relocateWithPrefix(shadePrefix, "org.tukaani")
    relocateWithPrefix(shadePrefix, "org.xerial.snappy")
    relocateWithPrefix(shadePrefix, "org.yaml")
    // NOTE: Do NOT shade log4j, otherwise logging won't work

    relocateAsyncHttpClientProperties(shadePrefix)
}
