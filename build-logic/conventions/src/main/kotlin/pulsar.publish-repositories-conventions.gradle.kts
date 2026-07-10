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

// Convention plugin for the Maven publishing TARGETS shared by every published Pulsar module:
// the publish repositories (local + ASF Nexus), the snapshot/release validation, the upload
// serialization lock, and GPG signing. It deliberately does NOT create any publication or touch the
// `base`/`java` plugins, so it can be applied both to the root project (which publishes only the
// POM-only `org.apache.pulsar:pulsar` parent artifact) and, via pulsar.publish-conventions, to every
// java-library / java-platform module. Applying it everywhere keeps the parent POM and all modules
// pointing at the same repositories and sharing the same upload lock, so a single `publish...` run
// uploads the whole set into one Nexus staging repository.

import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters

plugins {
    `maven-publish`
    signing
}

// Local Maven repository for testing/comparison.
publishing {
    repositories {
        maven {
            name = "localDeploy"
            url = uri(rootProject.layout.buildDirectory.dir("local-deploy-repo"))
        }
    }
}

// --- Apache distribution repositories (ASF Nexus) ---
// Repository names follow the ASF parent POM (apache.releases.https / apache.snapshots.https).
// Publish with one of:
//   ./gradlew publishAllPublicationsToApacheSnapshotsRepository   (for -SNAPSHOT versions)
//   ./gradlew publishAllPublicationsToApacheReleasesRepository    (for release versions)
// Uploads to the Apache staging repository are serialized by the mavenPublishLock shared build
// service (defined below) so they all land in a single Nexus staging repository: Nexus creates an
// implicit staging repository on first upload, and concurrent per-module uploads could otherwise be
// split across multiple implicitly-created staging repositories. Because the lock handles this,
// --no-parallel is not required.
// Credentials are resolved by Gradle at execution time from the apacheReleasesUsername /
// apacheReleasesPassword and apacheSnapshotsUsername / apacheSnapshotsPassword Gradle properties
// (the credentials(PasswordCredentials::class) form, which keeps the publish tasks
// configuration-cache compatible — explicitly assigned credentials would not be). Pass them as
// ORG_GRADLE_PROJECT_-prefixed environment variables on the publish command line so the password
// doesn't have to be stored in ~/.gradle/gradle.properties where it could leak to unrelated
// builds; start the command line with a space to keep the password out of shell history:
//    ORG_GRADLE_PROJECT_apacheReleasesUsername=$APACHE_USER \
//    ORG_GRADLE_PROJECT_apacheReleasesPassword="<your ASF password>" \
//    ./gradlew publishAllPublicationsToApacheReleasesRepository ...
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
    // Match on the task name (publish<Pub>PublicationTo<Repo>Repository) rather than reading
    // task.repository, which can still be null while the task is being created (e.g. for the root
    // project's POM-only publication). The captured booleans/strings keep this CC-compatible.
    val projectVersion = version.toString()
    val isSnapshotVersion = projectVersion.endsWith("-SNAPSHOT")
    tasks.withType<PublishToMavenRepository>().configureEach {
        val targetsApacheSnapshots = name.endsWith("ToApacheSnapshotsRepository")
        val targetsApacheReleases = name.endsWith("ToApacheReleasesRepository")
        if (targetsApacheSnapshots || targetsApacheReleases) {
            doFirst {
                if (targetsApacheSnapshots && !isSnapshotVersion) {
                    throw GradleException(
                        "Refusing to publish non-snapshot version '$projectVersion' to the " +
                            "'apacheSnapshots' repository. Use " +
                            "publishAllPublicationsToApacheReleasesRepository for release versions."
                    )
                }
                if (targetsApacheReleases && isSnapshotVersion) {
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

// --- Serialize uploads to Maven repositories ---
// Nexus creates an implicit staging repository on the first upload of a release, and concurrent
// per-module uploads can end up split across multiple implicitly-created staging repositories
// instead of being collected into a single one. A shared build service with maxParallelUsages = 1
// ensures at most one PublishToMavenRepository upload runs at a time across the whole build, so the
// rest of the build (compilation, jars, signing) can still run in parallel and --no-parallel is not
// required.
abstract class MavenPublishLock : BuildService<BuildServiceParameters.None>

val mavenPublishLock = gradle.sharedServices.registerIfAbsent(
    "mavenPublishLock", MavenPublishLock::class
) {
    maxParallelUsages = 1
}

tasks.withType<PublishToMavenRepository>().configureEach {
    usesService(mavenPublishLock)
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
