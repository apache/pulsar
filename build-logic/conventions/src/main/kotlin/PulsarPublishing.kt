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
import org.gradle.api.attributes.Usage
import org.gradle.api.plugins.BasePluginExtension
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.jvm.tasks.Jar
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType

/** The name of the Maven publication of each published Pulsar module, created by pulsar.publish-conventions. */
const val PULSAR_PUBLICATION = "maven"

/**
 * The archives name, and so the published artifactId, of a module that doesn't choose another one with
 * [publishedArtifactId]: its project name, qualified with its parent's name for modules nested two levels
 * deep, to match the Maven artifactIds. A project name that already starts with the parent's name, as the
 * names that settings.gradle.kts qualifies to avoid clashes do, is left as it is.
 */
fun Project.defaultArchivesName(): String {
    val parentProject = parent
    return if (parentProject != null && parentProject != rootProject && parentProject.parent != rootProject &&
            !name.startsWith(parentProject.name)) {
        "${parentProject.name}-$name"
    } else {
        name
    }
}

/**
 * Publishes this module under [artifactId] instead of its [defaultArchivesName], and names its archives
 * after it too, which also makes it the module's identity in the BOM and the NAR exclusions.
 */
fun Project.publishedArtifactId(artifactId: String) {
    extensions.getByType<BasePluginExtension>().archivesName.set(artifactId)
    extensions.getByType<PublishingExtension>().publications.withType<MavenPublication>().configureEach {
        this.artifactId = artifactId
    }
}

/** Configures this module's Maven publication, when the module has one (see PulsarApiSpiPublication). */
fun Project.configurePulsarPublication(action: MavenPublication.() -> Unit) {
    extensions.getByType<PublishingExtension>().publications.withType<MavenPublication>()
        .matching { it.name == PULSAR_PUBLICATION }
        .configureEach(action)
}

/**
 * Publishes the software component [componentName] of this Java library, with its sources and javadoc
 * JARs, and the test JAR of the modules that the Maven build published one for.
 */
fun Project.publishJavaLibrary(componentName: String) {
    val sourceSets = extensions.getByType<SourceSetContainer>()
    val sourcesJar = tasks.register<Jar>("sourcesJar") {
        archiveClassifier.set("sources")
        from(sourceSets.getByName("main").allJava)
    }
    val javadocJar = tasks.register<Jar>("javadocJar") {
        archiveClassifier.set("javadoc")
        from(tasks.named(JavaPlugin.JAVADOC_TASK_NAME))
    }
    val publishesTestJar = path in setOf(
        ":managed-ledger",
        ":pulsar-broker",
        ":pulsar-broker-common",
        ":pulsar-metadata",
        ":pulsar-package-management:pulsar-package-core",
    )
    configurePulsarPublication {
        from(components.getByName(componentName))
        artifact(sourcesJar)
        artifact(javadocJar)
        // Preserve the test JAR publications from the branch-4.2 Maven build.
        if (publishesTestJar) {
            artifact(tasks.named("testJar"))
        }
    }
}

/**
 * Fails the publishing of this module when it depends on a module that isn't published, in the
 * configurations that end up in the published POM. Test and compileOnly dependencies don't.
 * API/SPI mode validates the generated consumer metadata of the whole selection instead.
 */
fun Project.validatePublishedDependencies(publishedScopes: List<String>) {
    if (PulsarApiSpiPublication.isEnabled(this)) {
        return
    }
    val currentProjectPath = path
    // The configurations are looked up when the provider is evaluated, since a plugin applied later,
    // such as pulsar.client-shade-conventions with its shadowApi, can add them.
    val unpublishedDeps = provider {
        publishedScopes.mapNotNull { name -> configurations.findByName(name)?.let { name to it } }.flatMap { (configName, config) ->
            config.dependencies.withType(ProjectDependency::class.java)
                .filter { !rootProject.project(it.path).plugins.hasPlugin("maven-publish") }
                .map { "  - $configName -> ${it.path} (not published)" }
        }
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

/** Publishes the resolved versions of the `java` component's dependencies, as Maven does. */
fun MavenPublication.useResolvedVersions() {
    versionMapping {
        usage(Usage.JAVA_RUNTIME) {
            fromResolutionResult()
        }
        usage(Usage.JAVA_API) {
            fromResolutionOf("runtimeClasspath")
        }
    }
}
