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

// --- java-library projects: JAR + sources + javadoc ---
pluginManager.withPlugin("java-library") {
    val sourceSets = the<SourceSetContainer>()

    // Match Maven's javadoc configuration: no doclint, don't fail on errors
    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).apply {
            addStringOption("Xdoclint:none", "-quiet")
        }
        isFailOnError = false
    }

    val sourcesJar by tasks.registering(Jar::class) {
        archiveClassifier.set("sources")
        from(sourceSets["main"].allJava)
    }

    val javadocJar by tasks.registering(Jar::class) {
        archiveClassifier.set("javadoc")
        from(tasks.named(JavaPlugin.JAVADOC_TASK_NAME))
    }

    // The Maven publication. Its software component is wired in a deferred block because it depends
    // on whether this is a shaded module:
    //   - Shaded modules (com.gradleup.shadow applied) publish the shadow plugin's dependency-reduced
    //     `shadow` component: the artifact is the shadow jar and the published dependencies are
    //     EXACTLY the `shadow` configuration (the non-bundled runtime deps), not the bundled
    //     component modules + their unshaded transitive tree. This is the Gradle equivalent of
    //     Maven's createDependencyReducedPom.
    //   - All other java-library modules publish the standard `java` component with resolved
    //     version mapping (unchanged behavior).
    val mavenPublication = publishing.publications.create<MavenPublication>("maven") {
        artifact(sourcesJar)
        artifact(javadocJar)
    }

    // Shaded modules: the shadow plugin registers components["shadow"] in its own afterEvaluate, so
    // wire it from an afterEvaluate registered AFTER that. Registering inside withPlugin (which fires
    // once the shadow plugin is applied, after this convention) guarantees the ordering.
    pluginManager.withPlugin("com.gradleup.shadow") {
        afterEvaluate {
            mavenPublication.from(components["shadow"])
        }
    }

    // Non-shaded modules: standard java component + resolved version mapping. By afterEvaluate all
    // plugins are applied, so the shadow-plugin check is reliable.
    afterEvaluate {
        if (!pluginManager.hasPlugin("com.gradleup.shadow")) {
            mavenPublication.from(components["java"])
            mavenPublication.versionMapping {
                usage(Usage.JAVA_RUNTIME) {
                    fromResolutionResult()
                }
                usage(Usage.JAVA_API) {
                    fromResolutionOf("runtimeClasspath")
                }
            }
        }
    }
}

// --- java-platform projects (BOM, dependencies): POM-only, no JAR ---
pluginManager.withPlugin("java-platform") {
    publishing {
        publications {
            create<MavenPublication>("maven") {
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
    val isPlatformProject = plugins.hasPlugin("java-platform")
    val isRootProject = project == rootProject
    val pulsarVersion = version.toString()

    // Per-module POM name and description. Read in afterEvaluate so that a description
    // assigned in a module's build script body is picked up, and captured as plain strings
    // so the pom configuration stays configuration-cache compatible.
    if (!isRootProject) {
        afterEvaluate {
            val projectDescription = project.description ?: "Apache Pulsar :: $projectName"
            publishing.publications.withType<MavenPublication>().configureEach {
                pom {
                    name.set(projectDescription)
                    description.set(projectDescription)
                }
            }
        }
    }

    // Set the published artifactId from archivesName. Deferred to afterEvaluate (per the Gradle
    // maven-publish "deferred configuration" guidance) so a module can override archivesName in its
    // build script body — e.g. the shaded client modules publish under their historical Maven
    // artifactId ("pulsar-client", "pulsar-client-admin"). Read eagerly at plugin-application time
    // the override would be missed. Captured as a plain string for configuration-cache safety.
    afterEvaluate {
        val archivesNameValue = the<BasePluginExtension>().archivesName.get()
        publishing.publications.withType<MavenPublication>().configureEach {
            artifactId = archivesNameValue
        }
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
                        // Remove dependencyManagement from non-platform POMs
                        // (platform POMs need it — their dependencies ARE the management section)
                        if (!isPlatformProject) {
                            s = s.replace(
                                Regex(
                                    "<dependencyManagement>.*?</dependencyManagement>",
                                    RegexOption.DOT_MATCHES_ALL
                                ),
                                ""
                            )
                        }
                        // Inject <parent> reference for child modules (not the root/parent POM itself).
                        // Metadata (license, SCM, etc.) is inherited from the parent POM.
                        if (!isRootProject) {
                            s = s.replace(
                                "<modelVersion>4.0.0</modelVersion>",
                                "<modelVersion>4.0.0</modelVersion>\n  <parent>\n" +
                                    "    <groupId>org.apache.pulsar</groupId>\n" +
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

