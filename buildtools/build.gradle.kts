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
    implementation(libs.snakeyaml)
    implementation(libs.ant)
    implementation(libs.guava)
    implementation(libs.guice)
    implementation(libs.testng) {
        exclude(group = "org.slf4j")
    }
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.log4j.slf4j2.impl)
    implementation(libs.jcl.over.slf4j)
    implementation(libs.commons.lang3)

    // Netty is needed at runtime for ExtendedNettyLeakDetector and FastThreadLocalCleanupListener.
    // Using runtimeOnly so it propagates transitively when other modules depend on buildtools.
    compileOnly(libs.netty.common)
    runtimeOnly(libs.netty.common)
    compileOnly(libs.netty.buffer)
    runtimeOnly(libs.netty.buffer)

    implementation(libs.mockito.core)

    testImplementation(libs.netty.common)
    testRuntimeOnly(libs.netty.buffer)
}

tasks.withType<Test> {
    // Exclude inner classes used as test fixtures by BetweenTestClassesListenerAdapterTest.
    // They are run programmatically inside the test, not meant to be discovered directly.
    exclude("**/BetweenTestClassesListenerAdapterTest\$*")
}
