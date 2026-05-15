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
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":managed-ledger"))
    implementation(project(":pulsar-common"))
    implementation(project(":pulsar-broker"))
    implementation(libs.bookkeeper.server)
    implementation(libs.guava)
    implementation("org.openjdk.jmh:jmh-core:1.37")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")
}

// Don't build the benchmarks fat JAR during ./gradlew assemble — only on demand
shadow {
    addShadowJarToAssembleLifecycle.set(false)
}

tasks.shadowJar {
    archiveClassifier.set("benchmarks")
    isZip64 = true
    mergeServiceFiles()
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    manifest {
        attributes("Main-Class" to "org.openjdk.jmh.Main")
    }
}
