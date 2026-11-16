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

import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency

/**
 * The projects that apply the Shadow plugin. Compiling a project that depends on one builds that
 * project's shadow jar first, which `sanityCheck` avoids. The set is declared here, rather than found
 * by looking at the other projects' plugins, so that a project can tell without configuring or reading
 * other projects; pulsar.java-conventions fails the build when a project applies the Shadow plugin
 * without being listed.
 */
object PulsarShadedProjects {
    val paths: Set<String> = setOf(
        ":jclouds-shaded",
        ":jetty-upgrade:zookeeper-with-patched-admin",
        ":microbench",
        ":pulsar-broker-fastutil-minimized",
        ":pulsar-client-admin-shaded",
        ":pulsar-client-all",
        ":pulsar-client-fastutil-minimized",
        ":pulsar-client-shaded",
        ":pulsar-client-v5-shaded",
        ":pulsar-functions:pulsar-functions-local-runner-shaded",
        ":pulsar-functions:pulsar-functions-runtime-all",
        ":tests:java-test-functions",
    )

    /** The configurations whose project dependencies end up on a compile classpath. */
    private val compileScopeConfigurations = setOf(
        "api", "implementation", "compileOnly", "testImplementation", "testCompileOnly",
    )

    /** Fails when [project] applies the Shadow plugin without being in [paths]. */
    fun checkListed(project: Project) {
        if (project.path !in paths) {
            throw GradleException("Project '${project.path}' applies the Shadow plugin: add it to " +
                "PulsarShadedProjects.paths in build-logic.")
        }
    }

    /** Whether compiling [project]'s main or test sources needs the shadow jar of another project. */
    fun compilationNeedsShadowJar(project: Project): Boolean =
        project.configurations
            .filter { it.name in compileScopeConfigurations }
            .any { configuration ->
                configuration.dependencies.withType(ProjectDependency::class.java).any { it.path in paths }
            }
}
