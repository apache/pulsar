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

description = "Bouncy Castle Provider Loader"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

dependencies {
    compileOnly(project(":pulsar-common")) {
        exclude(group = "io.prometheus", module = "simpleclient_caffeine")
    }
    implementation(libs.bcpkix.jdk18on)
    implementation(libs.bcprov.ext.jdk18on)
    implementation(libs.slf4j.api)
}

// The Maven build uses executable-packer-maven-plugin to embed BC JARs
// preserving their signatures. In Gradle, we create a custom JAR that
// embeds the BC jars as resources (jar-in-jar).
val bcJarInJar by tasks.registering(Jar::class) {
    archiveClassifier.set("pkg")
    from(sourceSets.main.get().output)

    // Embed BC provider jars as resources to preserve signatures
    from(configurations.runtimeClasspath.get().filter {
        it.name.contains("bcprov") || it.name.contains("bcpkix") ||
        it.name.contains("bcutil") || it.name.contains("bcprov-ext")
    }) {
        into("META-INF/lib")
    }
}

artifacts {
    add("archives", bcJarInJar)
}
