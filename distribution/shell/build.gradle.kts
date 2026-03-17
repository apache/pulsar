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
    id("distribution")
    `java-library`
}

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

group = "org.apache.pulsar"
version = rootProject.version
description = "Pulsar Shell Distribution"

repositories {
    mavenCentral()
    maven { url = uri("https://packages.confluent.io/maven/") }
}

dependencies {
    implementation(platform(libs.netty.bom))
    implementation(platform(libs.jackson.bom))
    implementation(platform(libs.grpc.bom))
    implementation(platform(libs.slf4j.bom))
    implementation(platform(libs.prometheus.bom))
    implementation(platform(libs.jjwt.bom))

    implementation(project(":pulsar-client-tools"))
    implementation(project(":pulsar-client-shaded"))
    implementation(project(":pulsar-client-admin-shaded"))
}

// Ensure distribution tasks wait for shadow JARs from shaded dependencies
tasks.withType<AbstractArchiveTask>().configureEach {
    configurations.findByName("runtimeClasspath")?.allDependencies
        ?.withType<ProjectDependency>()
        ?.forEach { dep ->
            dep.dependencyProject.tasks.findByName("shadowJar")?.let { dependsOn(it) }
        }
}

distributions {
    main {
        distributionBaseName.set("apache-pulsar-shell")
        contents {
            from(configurations.runtimeClasspath) {
                into("lib")
            }
        }
    }
}
