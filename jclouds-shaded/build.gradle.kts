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

// Pin guava to 32.x for jclouds compatibility — jclouds uses TypeToken from
// com.google.common.reflect which was removed in guava 33.x.
// jclouds 2.6.0 requires Jakarta EE 10+ APIs (jakarta namespace) and Guice 7.x.
// The enforced platform pins older javax-era versions for Pulsar, but jclouds-shaded
// bundles its own copies so they won't conflict at runtime.
configurations.all {
    resolutionStrategy {
        force("com.google.guava:guava:32.0.0-jre")
        force("com.google.inject:guice:7.0.0")
        force("com.google.inject.extensions:guice-assistedinject:7.0.0")
        force("jakarta.annotation:jakarta.annotation-api:3.0.0")
        force("jakarta.ws.rs:jakarta.ws.rs-api:3.1.0")
        force("jakarta.inject:jakarta.inject-api:2.0.1")
    }
}

dependencies {
    implementation(libs.jclouds.allblobstore)
    implementation("org.apache.jclouds.driver:jclouds-apachehc:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.driver:jclouds-okhttp:${libs.versions.jclouds.get()}") {
        exclude(group = "com.squareup.okhttp")
    }
    implementation("org.apache.jclouds.driver:jclouds-slf4j:${libs.versions.jclouds.get()}")
}

tasks.shadowJar {
    dependencies {
        include(dependency("com.google.guava:guava"))
        include(dependency("com.google.guava:failureaccess"))
        include(dependency("org.apache.jclouds:.*"))
        include(dependency("org.apache.jclouds.api:.*"))
        include(dependency("org.apache.jclouds.common:.*"))
        include(dependency("org.apache.jclouds.driver:.*"))
        include(dependency("org.apache.jclouds.provider:.*"))
        include(dependency("com.google.inject.extensions:guice-assistedinject"))
        include(dependency("com.google.inject:guice"))
        include(dependency("com.google.inject.extensions:guice-multibindings"))
        include(dependency("com.google.code.gson:gson"))
        include(dependency("org.apache.httpcomponents:.*"))
        include(dependency("com.jamesmurty.utils:.*"))
        include(dependency("net.iharder:.*"))
        include(dependency("aopalliance:.*"))
        include(dependency("com.google.errorprone:.*"))
        include(dependency("jakarta.inject:jakarta.inject-api"))
        include(dependency("jakarta.annotation:jakarta.annotation-api"))
        include(dependency("jakarta.ws.rs:jakarta.ws.rs-api"))
    }

    // Exclude vulnerable gson embedded in jclouds-core uber jar
    exclude("lib/gson*jar")

    // No relocations needed — the NAR classloader provides isolation.
    // The shadow jar bundles guava 32.x (forced above) which has TypeToken
    // that jclouds needs. Relocating com.google would break jclouds' public
    // API (e.g. credentialsSupplier expects com.google.common.base.Supplier).
}

