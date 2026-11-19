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

import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.dsl.DependencyHandler

// The test jar of a module (the Maven test-jar equivalent): its test classes, for the tests of other
// modules. pulsar.java-conventions offers it as a variant with this capability, which a dependency on it
// requests, the way Gradle's java-test-fixtures plugin offers test fixtures.

/** The capability of the test jar variant of the project [projectPath], without a version. */
fun testJarCapability(projectPath: String): String =
    "org.apache.pulsar.test-jar:" + projectPath.removePrefix(":").replace(':', '.')

/** A dependency on the test jar of the project [projectPath], such as `testImplementation(testJar(":pulsar-broker"))`. */
fun DependencyHandler.testJar(projectPath: String): Dependency =
    (project(mapOf("path" to projectPath)) as ModuleDependency).capabilities {
        requireCapability(testJarCapability(projectPath))
    }
