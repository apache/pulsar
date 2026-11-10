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

import org.gradle.api.NamedDomainObjectProvider
import org.gradle.api.Project
import org.gradle.api.artifacts.ResolvableConfiguration
import org.gradle.api.attributes.Attribute
import org.gradle.api.attributes.AttributeContainer
import org.gradle.api.attributes.Category
import org.gradle.kotlin.dsl.named

// Build outputs that one project needs from another, such as a Docker image's ID file or an
// installed distribution, shared through configurations, which is how Gradle shares outputs between
// projects: the consumer resolves a project dependency, which builds the producing task and gives the
// consumer its files as inputs, without either project reaching into the other's tasks or layout.
// See https://docs.gradle.org/current/userguide/cross_project_publications.html

/** Which shared build artifact a variant holds, such as `docker-image:pulsar`. */
val SHARED_BUILD_ARTIFACT_ATTRIBUTE: Attribute<String> =
    Attribute.of("org.apache.pulsar.shared-build-artifact", String::class.java)

/**
 * The category of the shared build artifacts. Java libraries' variants have the `library` category,
 * so a project that is also a Java library never offers them for a shared build artifact.
 */
private const val SHARED_BUILD_ARTIFACT_CATEGORY = "pulsar-shared-build-artifact"

private fun Project.sharedBuildArtifactAttributes(attributes: AttributeContainer, name: String) {
    attributes.attribute(Category.CATEGORY_ATTRIBUTE, objects.named<Category>(SHARED_BUILD_ARTIFACT_CATEGORY))
    attributes.attribute(SHARED_BUILD_ARTIFACT_ATTRIBUTE, name)
}

private fun configurationName(name: String, suffix: String): String =
    name.split(':', '-', '.').filter { it.isNotEmpty() }
        .mapIndexed { index, part -> if (index == 0) part else part.replaceFirstChar(Char::uppercaseChar) }
        .joinToString("") + suffix

/**
 * Offers [artifact], which [builtBy] produces, to other projects as the shared build artifact [name].
 * [artifact] is anything that `ConfigurationPublications.artifact` accepts, such as a file provider.
 */
fun Project.shareBuildArtifact(name: String, artifact: Any, builtBy: Any) {
    configurations.consumable(configurationName(name, "Elements")) {
        sharedBuildArtifactAttributes(attributes, name)
        outgoing.artifact(artifact) {
            builtBy(builtBy)
        }
    }
}

/**
 * The shared build artifact [name] of the project [projectPath]. Using it as a task's input builds
 * the artifact first. Asking for the same artifact again returns the same configuration.
 */
fun Project.sharedBuildArtifact(projectPath: String, name: String): NamedDomainObjectProvider<ResolvableConfiguration> =
    sharedBuildArtifacts(listOf(projectPath), name)

/**
 * The shared build artifacts [name] of the projects [projectPaths], each of which shares one. Using it
 * as a task's input builds the artifacts first. Asking for the same artifacts again returns the same
 * configuration.
 */
fun Project.sharedBuildArtifacts(projectPaths: Collection<String>, name: String): NamedDomainObjectProvider<ResolvableConfiguration> {
    val resolvableName = configurationName(name, "")
    if (resolvableName in configurations.names) {
        return configurations.named(resolvableName, ResolvableConfiguration::class.java)
    }
    val declared = configurations.dependencyScope(configurationName(name, "Dependencies"))
    projectPaths.forEach { dependencies.add(declared.name, dependencies.project(mapOf("path" to it))) }
    return configurations.resolvable(resolvableName) {
        extendsFrom(declared.get())
        isTransitive = false
        sharedBuildArtifactAttributes(attributes, name)
    }
}
