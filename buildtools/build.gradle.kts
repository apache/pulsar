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

description = "Pulsar Build Tools - Test utilities, checkstyle configs, and test listeners"

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
    compileOnly(libs.netty.common)
    compileOnly(libs.netty.buffer)
    testImplementation(libs.netty.common)
    implementation(libs.mockito.core)
}

tasks.test {
    // Exclude inner test classes that are designed to be run programmatically by
    // BetweenTestClassesListenerAdapterTest, not directly by the test runner
    exclude("**/BetweenTestClassesListenerAdapterTest\$FailingAfterClassMethod*")
    exclude("**/BetweenTestClassesListenerAdapterTest\$TimeoutAndAfterClassMethod*")
    exclude("**/BetweenTestClassesListenerAdapterTest\$Base*")
    exclude("**/BetweenTestClassesListenerAdapterTest\$NoAfterClassMethods*")
    exclude("**/BetweenTestClassesListenerAdapterTest\$OneAfterClassMethod*")
    exclude("**/BetweenTestClassesListenerAdapterTest\$MultipleAfterClassMethods*")
    exclude("**/BetweenTestClassesListenerAdapterTest\$DisabledAfterClassMethod*")
    exclude("**/BetweenTestClassesListenerAdapterTest\$FactoryMethodCase*")
    exclude("**/BetweenTestClassesListenerAdapterTest\$FactoryMethodCaseWithoutAfterClass*")
}
