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

dependencies {
    implementation(libs.slog)
    compileOnly(project(":pulsar-client-tools"))
    compileOnly(project(":pulsar-broker"))

    testImplementation(project(":pulsar-client-tools"))
    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testJar"))
    testImplementation(project(":testmocks"))
    testImplementation(libs.awaitility)
    testImplementation(libs.jackson.databind)
    testImplementation(libs.guava)
    testImplementation(project(":pulsar-client-admin-original"))
    testImplementation(project(":pulsar-client-original"))
    testImplementation(project(":pulsar-functions:pulsar-functions-api"))
    testImplementation(libs.picocli)
}

// Copy the custom commands NAR from the example module into test resources
val copyCustomCommandsNar by tasks.registering(Copy::class) {
    dependsOn(":pulsar-client-tools-customcommand-example:nar")
    from(project(":pulsar-client-tools-customcommand-example").layout.buildDirectory.dir("libs"))
    include("customCommands-nar.nar")
    into(layout.buildDirectory.dir("resources/test/cliextensions"))
}

tasks.withType<Test> {
    dependsOn(copyCustomCommandsNar)
}

// checkstyleTest also scans test resources — ensure NAR copy runs first
plugins.withId("checkstyle") {
    tasks.named("checkstyleTest") {
        dependsOn(copyCustomCommandsNar)
    }
}
