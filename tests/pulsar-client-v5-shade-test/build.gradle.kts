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

import org.gradle.api.attributes.Bundling

plugins {
    id("pulsar.java-conventions")
}

dependencies {
    // Exercise the published shaded variant without original implementation dependencies.
    testImplementation(project(":pulsar-client-v5-shaded")) {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.SHADOWED))
        }
    }
    testImplementation(libs.testcontainers)
}

// Run the same end-to-end admin/v5 scenario with only the combined shaded artifact.
// Reuse source files without reading or configuring another project's model.
sourceSets.test {
    java.srcDir("../pulsar-client-admin-v5-test/src/test/java")
    java.include("**/AdminV5MessagingTest.java", "**/PulsarContainer.java", "**/ClientClasspathTest.java")
}

tasks.named<Test>("test") {
    systemProperty("testShadedClient", "true")
}
