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
    id("pulsar.public-java-library-conventions")
}

dependencies {
    implementation(libs.log4j.core)
    implementation(libs.prometheus.metrics.core)
    // Writes the plugin index that lets log4j2 find the appender without a "packages" attribute
    annotationProcessor(libs.log4j.core)

    testImplementation(libs.prometheus.metrics.exposition.textformats)
}

tasks.compileJava {
    // Names the directory of the GraalVM reflection metadata that the log4j2 plugin processor writes
    options.compilerArgs.addAll(
        listOf("-Alog4j.graalvm.groupId=${project.group}", "-Alog4j.graalvm.artifactId=${project.name}")
    )
}
