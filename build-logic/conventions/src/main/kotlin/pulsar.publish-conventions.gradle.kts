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

// Convention plugin for the Maven PUBLICATION of each published Pulsar module: the software
// component, POM metadata, sources/javadoc JARs, and the artifactId. The publish repositories
// (local + ASF Nexus), GPG signing, the upload-serialization lock and the snapshot/release
// validation are shared with the root parent-POM project via pulsar.publish-repositories-conventions.

plugins {
    id("pulsar.publish-repositories-conventions")
}

// --- java-library projects ---
// The `maven` publication, without contents: what it publishes depends on the module's packaging,
// and the convention plugin that defines the packaging adds it, see PulsarPublishing.kt:
//   - pulsar.public-java-library-conventions: the `java` component, with sources and javadoc;
//   - pulsar.public-shaded-library-conventions: the dependency-reduced `shadow` component, with
//     sources and javadoc;
//   - pulsar.nar-conventions: only the .nar archive.
// That keeps this plugin independent of the order in which the plugins are applied.
pluginManager.withPlugin("java-library") {
    if (!PulsarApiSpiPublication.includes(project)) {
        return@withPlugin
    }

    // Match Maven's javadoc configuration: no doclint, don't fail on errors
    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).apply {
            addStringOption("Xdoclint:none", "-quiet")
        }
        isFailOnError = false
    }

    publishing.publications.create<MavenPublication>(PULSAR_PUBLICATION) {
        pom {
            // dependencyManagement only belongs in platform POMs, whose dependencies ARE the
            // management section.
            withXml {
                val sb = asString()
                val s = sb.toString().replace(
                    Regex("<dependencyManagement>.*?</dependencyManagement>", RegexOption.DOT_MATCHES_ALL),
                    "",
                )
                sb.setLength(0)
                sb.append(s)
            }
        }
    }
}

// --- java-platform projects (BOM, dependencies): POM-only, no JAR ---
pluginManager.withPlugin("java-platform") {
    if (!PulsarApiSpiPublication.includes(project)) {
        return@withPlugin
    }
    publishing {
        publications {
            create<MavenPublication>(PULSAR_PUBLICATION) {
                from(components["javaPlatform"])
            }
        }
    }
}

// --- Common POM configuration for all publications ---
run {
    // Capture values in a local scope so withXml closures don't capture the script object
    // (which would break configuration cache serialization)
    val projectName = project.name
    val isRootProject = project == rootProject
    val pulsarVersion = version.toString()
    val pulsarGroup = project.group.toString()

    // Per-module POM name and description. A provider, so that a description assigned in a
    // module's build script body is picked up.
    if (!isRootProject) {
        val projectDescription = provider { project.description ?: "Apache Pulsar :: $projectName" }
        publishing.publications.withType<MavenPublication>().configureEach {
            pom {
                name.set(projectDescription)
                description.set(projectDescription)
            }
        }
    }

    // Publish under the name of the archives, which pulsar.java-conventions qualifies for nested
    // modules. MavenPublication.artifactId is not lazy, so it is computed the same way rather than
    // read from archivesName; a module that publishes under another name calls
    // publishedArtifactId(...), which sets both. See PulsarPublishing.kt.
    val defaultArtifactId = defaultArchivesName()
    publishing.publications.withType<MavenPublication>().configureEach {
        artifactId = defaultArtifactId
    }

    publishing {
        publications {
            withType<MavenPublication>().configureEach {
                pom {
                    // Clean up POM XML and inject <parent> reference
                    withXml {
                        val sb = asString()
                        var s = sb.toString()
                        // <scope>compile</scope> is the Maven default — remove for cleaner POM
                        s = s.replace("<scope>compile</scope>", "")
                        // Inject <parent> reference for child modules (not the root/parent POM itself).
                        // Metadata (license, SCM, etc.) is inherited from the parent POM.
                        if (!isRootProject) {
                            s = s.replace(
                                "<modelVersion>4.0.0</modelVersion>",
                                "<modelVersion>4.0.0</modelVersion>\n  <parent>\n" +
                                    "    <groupId>$pulsarGroup</groupId>\n" +
                                    "    <artifactId>pulsar</artifactId>\n" +
                                    "    <version>$pulsarVersion</version>\n" +
                                    "  </parent>"
                            )
                        }
                        sb.setLength(0)
                        sb.append(s)
                        // Re-format the XML
                        asNode()
                    }
                }
            }
        }
    }
}

// NOTE: the enforced-platform validation error is intentionally NOT suppressed. The internal
// version-alignment platform (:pulsar-dependencies) is declared on the non-published
// `internalPlatform` bucket in pulsar.java-conventions (only the resolvable build classpaths
// extend it), so it never reaches the published apiElements/runtimeElements variants. Letting the
// validation run unsuppressed guards against the enforced platform regressing back into published
// Gradle Module Metadata.

