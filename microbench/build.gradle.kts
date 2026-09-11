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
    // No version: the Shadow plugin is already on the classpath via the build-logic conventions,
    // so a versioned request cannot be reconciled with it.
    id("com.gradleup.shadow")
}

dependencies {
    api(project(":managed-ledger"))
    implementation(project(":pulsar-common"))
    api(project(":pulsar-broker"))
    implementation(libs.bookkeeper.server)
    implementation(libs.fastutil)
    api(libs.guava)
    api("org.openjdk.jmh:jmh-core:1.37")
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
    // See pulsar.shadow-conventions: the default EXCLUDE strategy drops duplicates of
    // transformer-owned paths before the transformers can merge them.
    val transformedPaths = listOf("META-INF/services/**", "META-INF/*.kotlin_module")
    filesMatching(transformedPaths) {
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }
    inputs.property("transformedPathsDuplicatesStrategy", "$transformedPaths=INCLUDE")
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    manifest {
        attributes("Main-Class" to "org.openjdk.jmh.Main")
    }
}
