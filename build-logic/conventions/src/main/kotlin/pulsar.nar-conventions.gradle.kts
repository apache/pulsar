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

// Convention plugin for NAR (Nifi Archive) modules.
// Configures platform module exclusions from runtimeClasspath, forces JAR artifacts
// for bundled-dependencies, handles archive name qualification, and publishes the
// NAR artifact with an empty POM (no dependencies — everything is bundled).

plugins {
    id("io.github.merlimat.nar")
    id("pulsar.publish-conventions")
}

// NAR modules should not bundle Pulsar platform dependencies — they are provided
// at runtime by Pulsar's classloader hierarchy.
// Note: pulsar-io-common is NOT in java-instance.jar (runtime-all), so it must be
// bundled in each NAR that uses it (e.g., IOConfigUtils).
val pulsarPlatformModules = setOf(
    "pulsar-client-api",
    "pulsar-client-admin-api",
    "pulsar-client-original",
    "pulsar-client",
    "pulsar-common",
    "pulsar-config-validation",
    "bouncy-castle-bc",
    "pulsar-functions-api",
    "pulsar-functions-instance",
    "pulsar-functions-proto",
    "pulsar-functions-secrets",
    "pulsar-functions-utils",
    "pulsar-io-core",
    "pulsar-metadata",
    "pulsar-opentelemetry",
    "managed-ledger",
    "pulsar-package-core",
)

configurations.named("runtimeClasspath") {
    exclude(group = "org.apache.bookkeeper")
    // Protobuf is in java-instance.jar (runtime-all), so NARs must not bundle it.
    // Bundling a different version causes GeneratedMessage.getUnknownFields() conflicts.
    exclude(group = "com.google.protobuf")
    pulsarPlatformModules.forEach { module ->
        exclude(group = "org.apache.pulsar", module = module)
    }
}

// The NAR plugin copies from runtimeClasspath which resolves project dependencies
// as class directories, not JARs. The NarClassLoader expects JARs in
// META-INF/bundled-dependencies/. Force the NAR task to use JAR artifacts.
// Use lazy resolution to avoid eagerly resolving the configuration at configuration
// time, which would cause configuration cache invalidation when JARs are created.
tasks.named("nar", Jar::class.java) {
    val runtimeClasspath = configurations.named("runtimeClasspath")
    into("META-INF/bundled-dependencies") {
        from(runtimeClasspath.map { conf ->
            conf.incoming.artifactView {
                attributes {
                    attribute(
                        LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                        objects.named(LibraryElements::class.java, LibraryElements.JAR)
                    )
                }
            }.files
        })
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
}

// Set NAR-specific archive name qualification for nested modules.
val parentProject = project.parent
if (parentProject != null && parentProject != rootProject && parentProject.parent != rootProject
        && !project.name.startsWith(parentProject.name)) {
    val qualifiedName = "${parentProject.name}-${project.name}"
    val narExt = extensions.getByName("nar")
    @Suppress("UNCHECKED_CAST")
    val narIdProp = narExt.javaClass.getMethod("getNarId").invoke(narExt) as org.gradle.api.provider.Property<String>
    narIdProp.set(qualifiedName)
}

// --- NAR publishing: publish only the .nar artifact with an empty POM ---
// NAR modules bundle all dependencies, so the POM should have no <dependencies> section.
publishing {
    publications {
        named<MavenPublication>("maven") {
            // Replace component-based artifacts with just the NAR file
            artifacts.clear()
            artifact(tasks.named("nar"))
            pom {
                packaging = "nar"
                // Remove all dependencies — NAR bundles everything
                withXml {
                    val root = asNode()
                    root.children().removeAll { node ->
                        val name = (node as groovy.util.Node).name()
                        name.toString().contains("dependencies")
                    }
                }
            }
        }
    }
}
