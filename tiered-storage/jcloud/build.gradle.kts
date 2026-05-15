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
    id("pulsar.nar-conventions")
}
dependencies {
    implementation(libs.slog)
    compileOnly(project(":managed-ledger"))
    compileOnly(libs.bookkeeper.server)
    compileOnly(libs.netty.buffer)
    implementation(project(":jclouds-shaded")) {
        // Shadow jar contains all needed classes — prevent unshaded jclouds/guava from leaking
        isTransitive = false
    }
    compileOnly(libs.jclouds.allblobstore)
    compileOnly(libs.jclouds.blobstore)
    implementation(libs.aws.java.sdk.core)
    implementation(libs.aws.java.sdk.sts)
    runtimeOnly(libs.jakarta.xml.bind.api)
    runtimeOnly(libs.jakarta.activation)

    testImplementation(project(":managed-ledger"))
    testImplementation(project(":testmocks"))
    testImplementation(libs.guava)
    testImplementation(libs.netty.buffer)
    testCompileOnly(libs.jclouds.blobstore)
    testCompileOnly(libs.jclouds.allblobstore)
    // The "transient" blobstore provider (for in-memory tests) is registered via
    // META-INF/services in jclouds-allblobstore. We need it at runtime but must
    // exclude Guice transitives to avoid conflicts with the platform-enforced Guice.
    testRuntimeOnly(libs.jclouds.allblobstore) {
        exclude(group = "com.google.inject")
        exclude(group = "com.google.inject.extensions")
    }
    testImplementation(libs.simpleclient)
    // javax.inject is needed by bookkeeper and other deps on the test classpath
    testRuntimeOnly("javax.inject:javax.inject:1")
}

// jclouds 2.6.0 requires Guice 7.x and jakarta.annotation 3.x, both bundled in jclouds-shaded.
// Exclude the platform-enforced Guice 5.1.0 and jakarta.annotation-api 1.3.5 from the test
// runtime so the versions from jclouds-shaded are used instead.
configurations.testRuntimeClasspath {
    exclude(group = "com.google.inject", module = "guice")
    exclude(group = "com.google.inject.extensions", module = "guice-assistedinject")
    exclude(group = "jakarta.annotation", module = "jakarta.annotation-api")
    resolutionStrategy {
        force("jakarta.annotation:jakarta.annotation-api:3.0.0")
    }
}

tasks.test {
    // Set TTL for OffsetsCache before class loading so OffsetsCacheTest's TTL test works
    // even when other tests load the OffsetsCache class first in the same JVM fork
    jvmArgs("-Dpulsar.jclouds.readhandleimpl.offsetsscache.ttl.seconds=1")
}
