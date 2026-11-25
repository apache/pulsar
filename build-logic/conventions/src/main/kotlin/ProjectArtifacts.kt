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
import org.gradle.api.artifacts.DependencyScopeConfiguration
import org.gradle.api.artifacts.ResolvableConfiguration
import org.gradle.api.artifacts.type.ArtifactTypeDefinition
import org.gradle.api.attributes.AttributeContainer
import org.gradle.api.attributes.Bundling
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.kotlin.dsl.named

// Configurations that resolve the archives that other projects build, for tasks that copy them or pass
// their paths, such as Docker build contexts and tests that load a NAR. Each declares its dependencies
// in a `<name>Dependencies` bucket and resolves them in `<name>`, with the attributes of the variant it
// wants, rather than in a legacy configuration that is both and relies on the `default` configuration.

/** The two configurations of [projectJars] and [projectNars]: the dependencies and their files. */
class ProjectArtifacts(
    val dependencies: NamedDomainObjectProvider<DependencyScopeConfiguration>,
    val files: NamedDomainObjectProvider<ResolvableConfiguration>,
)

private fun Project.javaRuntimeLibrary(attributes: AttributeContainer) {
    attributes.attribute(Usage.USAGE_ATTRIBUTE, objects.named<Usage>(Usage.JAVA_RUNTIME))
    attributes.attribute(Category.CATEGORY_ATTRIBUTE, objects.named<Category>(Category.LIBRARY))
}

private fun Project.projectArtifacts(
    name: String,
    declaredName: String,
    projectPaths: Array<out String>,
    transitive: Boolean,
    configureAttributes: AttributeContainer.() -> Unit,
): ProjectArtifacts {
    val declared = configurations.dependencyScope(declaredName)
    projectPaths.forEach { dependencies.add(declared.name, dependencies.project(mapOf("path" to it))) }
    val resolvable = configurations.resolvable(name) {
        extendsFrom(declared.get())
        isTransitive = transitive
        attributes.configureAttributes()
    }
    return ProjectArtifacts(declared, resolvable)
}

private fun Project.runtimeJarAttributes(attributes: AttributeContainer) {
    javaRuntimeLibrary(attributes)
    attributes.attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named<LibraryElements>(LibraryElements.JAR))
    attributes.attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named<Bundling>(Bundling.EXTERNAL))
}

/**
 * Resolves the runtime JARs of the projects [projectPaths], the shaded JAR of a shaded project, without
 * their dependencies unless [transitive]. More dependencies can be added to [ProjectArtifacts.dependencies].
 */
fun Project.projectJars(name: String, vararg projectPaths: String, transitive: Boolean = false): ProjectArtifacts =
    projectArtifacts(name, "${name}Dependencies", projectPaths, transitive) { runtimeJarAttributes(this) }

/**
 * Resolves the runtime JARs, with their dependencies, of what is declared in the [declaredName]
 * configuration, such as the libraries that a distribution bundles.
 */
fun Project.runtimeJars(name: String, declaredName: String): ProjectArtifacts =
    projectArtifacts(name, declaredName, emptyArray(), transitive = true) { runtimeJarAttributes(this) }

/** Resolves the NAR archives of the NAR projects [projectPaths], which the NAR plugin's `narElements` offer. */
fun Project.projectNars(name: String, vararg projectPaths: String): ProjectArtifacts =
    projectArtifacts(name, "${name}Dependencies", projectPaths, transitive = false) {
        javaRuntimeLibrary(this)
        attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, "nar")
    }
