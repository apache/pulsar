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

// Convention plugin for public Pulsar Java libraries that are published to Maven repositories.
// Combines java-conventions (compilation, testing) with publish-conventions (Maven publishing,
// signing, POM metadata). Internal-only modules should use pulsar.java-conventions directly.

plugins {
    id("pulsar.java-conventions")
    id("pulsar.publish-conventions")
}

// Validate that public java-library modules only depend on other published modules
// in scopes that end up in the published POM (api, implementation, runtimeOnly).
// Test/compileOnly scoped dependencies are excluded since they don't appear in the POM.
// NAR modules are not validated here — they bundle all dependencies and have empty POMs.
run {
    val publishedScopes = listOf("api", "implementation", "runtimeOnly")
    val configsToCheck = publishedScopes.mapNotNull { name ->
        configurations.findByName(name)?.let { name to it }
    }
    val currentProjectPath = project.path

    val unpublishedDeps = provider {
        val errors = mutableListOf<String>()
        for ((configName, config) in configsToCheck) {
            for (dep in config.dependencies) {
                if (dep is ProjectDependency) {
                    val depPath = dep.path
                    val depProject = project.rootProject.project(depPath)
                    if (!depProject.plugins.hasPlugin("maven-publish")) {
                        errors.add("  - $configName -> $depPath (not published)")
                    }
                }
            }
        }
        errors
    }

    tasks.withType<PublishToMavenRepository>().configureEach {
        val errorList = unpublishedDeps
        doFirst {
            val errors = errorList.get()
            if (errors.isNotEmpty()) {
                throw GradleException(
                    "Published module '$currentProjectPath' depends on unpublished projects:\n" +
                        errors.joinToString("\n") + "\n" +
                        "Either publish the dependency or move it to a test/compileOnly scope."
                )
            }
        }
    }
}
