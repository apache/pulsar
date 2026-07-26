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
    alias(libs.plugins.graalvm.native)
}

// The CLI application (NativeImageTesterApp) is compiled to a native binary by the
// GraalVM native-build-tools plugin. The reachability metadata (reflect-config.json,
// resource-config.json, native-image.properties) is consumed automatically from the
// META-INF/native-image directories embedded in pulsar-client-original and
// pulsar-client-admin-original. The TestNG smoke test then runs on the JVM and drives
// the native binary via java.lang.ProcessBuilder against a Testcontainers Pulsar broker.

dependencies {
    // Use the "-original" (unshaded) clients: the embedded native-image config references
    // unshaded class names, so it only applies when compiling against these artifacts.
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-client-admin-original"))

    testImplementation(project(":buildtools"))
    testImplementation(libs.testcontainers)
}

val nativeImageName = "pulsar-client-native-tester"

graalvmNative {
    // Quick build mode keeps CI native-image compilation under the integration-test budget.
    // See https://www.graalvm.org/latest/reference-manual/native-image/overview/BuildOutput/
    binaries.all {
        quickBuild = true
    }
    binaries.named("main") {
        imageName = nativeImageName
        mainClass = "org.apache.pulsar.tests.integration.nativeimage.NativeImageTesterApp"
        buildArgs.add("--no-fallback")
    }
}

tasks.named<Test>("test") {
    useTestNG {
        suiteXmlFiles = listOf(file("src/test/resources/native-image-tests.xml"))
    }
    // The smoke test shells out to the compiled native binary, so build it first.
    dependsOn(tasks.named("nativeCompile"))
    val nativeBinary = layout.buildDirectory.file("native/nativeCompile/$nativeImageName")
    systemProperty("native.image.binary", nativeBinary.get().asFile.absolutePath)
}
