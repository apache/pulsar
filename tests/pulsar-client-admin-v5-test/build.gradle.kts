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

import org.gradle.api.attributes.Bundling

plugins {
    id("pulsar.java-conventions")
}

dependencies {
    testImplementation(project(":pulsar-client-v5"))
    // Select the published shaded variant, without the original implementation dependencies
    // exposed by the local Java component. Otherwise a project test can hide packaging bugs.
    testImplementation(project(":pulsar-client-admin-shaded")) {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.SHADOWED))
        }
    }
    testImplementation(libs.testcontainers)
}

// Run fresh JVMs in both orders: neither artifact may supply the other's implementation.
val testClasspath = sourceSets.test.get().runtimeClasspath
val testClasses = sourceSets.test.get().output.classesDirs
tasks.named<Test>("test") {
    classpath = files(testClasspath.elements.map { entries ->
        entries.sortedBy { it.asFile.name }
    })
}
val testReversedClasspath by tasks.registering(Test::class) {
    description = "Tests the v5 client and shaded admin jar in reverse classpath order."
    testClassesDirs = testClasses
    classpath = files(testClasspath.elements.map { entries ->
        entries.sortedByDescending { it.asFile.name }
    })
}
tasks.named("check") {
    dependsOn(testReversedClasspath)
}
