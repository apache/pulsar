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
    api(project(":pulsar-client-tools-api"))
    implementation(project(":pulsar-client-admin-api"))
    implementation(project(":pulsar-client-admin-original"))
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-common"))
    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(project(":pulsar-cli-utils"))
    implementation(project(":pulsar-websocket")) {
        exclude(group = "*", module = "*")
    }
    implementation(libs.picocli)
    implementation(libs.picocli.shell.jline3)
    implementation(libs.jline)
    implementation(libs.commons.io)
    implementation(libs.commons.lang3)
    implementation(libs.commons.text)
    implementation(libs.asynchttpclient)
    implementation(libs.netty.reactive.streams)
    implementation(libs.gson)
    implementation(libs.javassist)
    implementation(libs.avro)
    implementation(libs.jetty.client)
    implementation(libs.jetty.websocket.jetty.api)
    implementation(libs.jetty.websocket.jetty.client)
    runtimeOnly(libs.jna)

    compileOnly(libs.swagger.core)

    testImplementation(libs.jackson.dataformat.yaml)
    testImplementation(libs.guava)
}

// Maven uses ant-plugin to copy pom.xml -> dummy.nar for TestCmdSinks/TestCmdSources.
// The file is gitignored (*.nar), so we generate it from the build file instead.
// Must run after processTestResources to avoid being overwritten.
val generateDummyNar by tasks.registering(Copy::class) {
    dependsOn(tasks.named("processTestResources"))
    from(layout.projectDirectory.file("build.gradle.kts"))
    into(layout.buildDirectory.dir("resources/test"))
    rename { "dummy.nar" }
}

tasks.withType<Test> {
    dependsOn(generateDummyNar)
}
