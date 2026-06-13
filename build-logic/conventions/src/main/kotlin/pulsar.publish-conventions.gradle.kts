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

// Convention plugin for publishing Pulsar modules to Maven repositories.
// Configures maven-publish, GPG signing, POM metadata, sources/javadoc JARs,
// the ASF Nexus release/snapshot repositories, and a local deploy repository
// for testing.

plugins {
    `maven-publish`
    signing
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

    // Standard java-library modules: publish from components["java"]
    publishing {
        publications {
            create<MavenPublication>("maven") {
                from(components["java"])
                artifact(sourcesJar)
                artifact(javadocJar)

                versionMapping {
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
    val archivesNameValue = the<BasePluginExtension>().archivesName.get()
    val isPlatformProject = plugins.hasPlugin("java-platform")
    val isRootProject = project == rootProject
    val pulsarVersion = version.toString()
    val localDeployRepoDir = rootProject.layout.buildDirectory.dir("local-deploy-repo")

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

    publishing {
        publications {
            withType<MavenPublication>().configureEach {
                artifactId = archivesNameValue

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

        // Local Maven repository for testing/comparison
        repositories {
            maven {
                name = "localDeploy"
                url = uri(localDeployRepoDir)
            }
        }
    }
}

// --- Apache distribution repositories (ASF Nexus) ---
// Repository names follow the ASF parent POM (apache.releases.https / apache.snapshots.https).
// Publish with one of:
//   ./gradlew publishAllPublicationsToApacheSnapshotsRepository   (for -SNAPSHOT versions)
//   ./gradlew publishAllPublicationsToApacheReleasesRepository    (for release versions)
// Releases must be published with --no-parallel: when uploading to the Apache staging
// repository, Nexus creates an implicit staging repository, and concurrent per-module uploads
// can end up split across multiple implicitly-created staging repositories instead of being
// collected into a single one.
// Credentials are resolved by Gradle at execution time from the apacheReleasesUsername /
// apacheReleasesPassword and apacheSnapshotsUsername / apacheSnapshotsPassword Gradle properties
// (the credentials(PasswordCredentials::class) form, which keeps the publish tasks
// configuration-cache compatible — explicitly assigned credentials would not be). Pass them as
// ORG_GRADLE_PROJECT_-prefixed environment variables on the publish command line so the password
// doesn't have to be stored in ~/.gradle/gradle.properties where it could leak to unrelated
// builds; start the command line with a space to keep the password out of shell history:
//    ORG_GRADLE_PROJECT_apacheReleasesUsername=$APACHE_USER \
//    ORG_GRADLE_PROJECT_apacheReleasesPassword="<your ASF password>" \
//    ./gradlew publishAllPublicationsToApacheReleasesRepository --no-parallel ...
// The URLs can be overridden with the apacheReleasesRepoUrl / apacheSnapshotsRepoUrl Gradle
// properties (e.g. a file:// URL for testing the publication layout).
run {
    fun MavenArtifactRepository.configureApacheRepository(urlProperty: String, defaultUrl: String) {
        val repositoryUrl = uri(providers.gradleProperty(urlProperty).getOrElse(defaultUrl))
        url = repositoryUrl
        // The file transport (an URL overridden to file:// for testing) rejects credentials,
        // and Gradle's credentials validation would fail when the properties aren't set.
        if (repositoryUrl.scheme != "file") {
            credentials(PasswordCredentials::class)
        }
    }

    publishing {
        repositories {
            maven {
                name = "apacheReleases"
                configureApacheRepository(
                    "apacheReleasesRepoUrl",
                    "https://repository.apache.org/service/local/staging/deploy/maven2"
                )
            }
            maven {
                name = "apacheSnapshots"
                configureApacheRepository(
                    "apacheSnapshotsRepoUrl",
                    "https://repository.apache.org/content/repositories/snapshots"
                )
            }
        }
    }

    // Validate before any upload: only -SNAPSHOT versions may go to apacheSnapshots and only
    // release versions to apacheReleases. (Maven's deploy picks the repository from the version;
    // in Gradle the task name picks the repository, so the version must be checked instead.)
    // The task's repository property is discarded by configuration cache serialization, so
    // capture the repository name at configuration time and only register the validation
    // action (capturing plain strings/booleans) for the Apache repositories.
    val projectVersion = version.toString()
    val isSnapshotVersion = projectVersion.endsWith("-SNAPSHOT")
    tasks.withType<PublishToMavenRepository>().configureEach {
        val repositoryName = repository.name
        if (repositoryName == "apacheReleases" || repositoryName == "apacheSnapshots") {
            doFirst {
                if (repositoryName == "apacheSnapshots" && !isSnapshotVersion) {
                    throw GradleException(
                        "Refusing to publish non-snapshot version '$projectVersion' to the " +
                            "'apacheSnapshots' repository. Use " +
                            "publishAllPublicationsToApacheReleasesRepository for release versions."
                    )
                }
                if (repositoryName == "apacheReleases" && isSnapshotVersion) {
                    throw GradleException(
                        "Refusing to publish snapshot version '$projectVersion' to the " +
                            "'apacheReleases' repository. Use " +
                            "publishAllPublicationsToApacheSnapshotsRepository for -SNAPSHOT versions."
                    )
                }
            }
        }
    }
}

// --- GPG signing ---
signing {
    isRequired = !version.toString().endsWith("-SNAPSHOT")

    val useGpgCmd = providers.gradleProperty("useGpgCmd").orNull?.toBoolean() ?: false
    if (useGpgCmd) {
        useGpgCmd()
    }

    sign(publishing.publications)
}

// Disable signing tasks when no signing configuration is present (local dev without signing).
// With -PuseGpgCmd=true, an explicit key isn't needed: the gpg command uses its default key
// unless -Psigning.gnupg.keyName=<key id> selects one.
tasks.withType<Sign>().configureEach {
    enabled = providers.gradleProperty("signing.keyId").isPresent ||
        providers.gradleProperty("signing.gnupg.keyName").isPresent ||
        (providers.gradleProperty("useGpgCmd").orNull?.toBoolean() ?: false)
}

// Suppress enforced-platform validation: all java-library modules use
// enforcedPlatform(":pulsar-dependencies") for internal version alignment,
// but this should not leak to consumers. The dependencyManagement section
// is stripped from published POMs via withXml above.
tasks.withType<GenerateModuleMetadata>().configureEach {
    suppressedValidationErrors.add("enforced-platform")
}

