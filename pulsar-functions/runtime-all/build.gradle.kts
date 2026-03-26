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

dependencies {
    implementation(project(":pulsar-io:pulsar-io-core"))
    implementation(project(":pulsar-functions:pulsar-functions-api"))
    implementation(project(":pulsar-client-api"))
    implementation(libs.avro)
    implementation(libs.jackson.databind)
    implementation(libs.protobuf.java)
    implementation(libs.gson)
    implementation(libs.slf4j.api)
    implementation(libs.log4j.slf4j2.impl)
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
}

// Build a fat JAR as java-instance.jar
tasks.jar {
    archiveFileName.set("java-instance.jar")
    // Use provider to properly declare task dependencies on upstream jars
    val runtimeCp = configurations.runtimeClasspath
    dependsOn(runtimeCp)
    from(provider { runtimeCp.get().map { if (it.isDirectory) it else zipTree(it) } }) {
        exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.withType<Test> {
    dependsOn(tasks.jar)
}
