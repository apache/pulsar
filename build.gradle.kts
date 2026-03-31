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
import com.github.vlsi.gradle.git.dsl.gitignore
import org.jetbrains.gradle.ext.copyright
import org.jetbrains.gradle.ext.settings

buildscript {
    // The license plugin pulls in plexus-utils:2.0.6 which conflicts with
    // the Shadow plugin's plexus-utils:4.0.2 (missing 4-arg matchPath method).
    // Force the newer version to avoid NoSuchMethodError at shading time.
    configurations.classpath {
        resolutionStrategy.force("org.codehaus.plexus:plexus-utils:4.0.2")
    }
}

plugins {
    alias(libs.plugins.rat)
    alias(libs.plugins.version.catalog.update)
    alias(libs.plugins.versions)
    alias(libs.plugins.crlf) apply false
    alias(libs.plugins.idea.ext)
    alias(libs.plugins.spotless) apply false // workaround for https://github.com/diffplug/spotless/issues/2877
}

versionCatalogUpdate {
    sortByKey = false
    keep {
        keepUnusedVersions.set(true)
    }
}

tasks.named<com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask>("dependencyUpdates") {
    outputFormatter = "html"
    rejectVersionIf {
        val nonStable = candidate.version.contains("alpha") || candidate.version.contains("beta") || candidate.version.contains("rc")
        // OpenTelemetry publishes stable releases with -alpha suffix for some modules
        val isOpenTelemetry = candidate.group.startsWith("io.opentelemetry")
        nonStable && !(isOpenTelemetry && candidate.version.contains("alpha"))
    }
}

val catalog = the<VersionCatalogsExtension>().named("libs")
val pulsarVersion = catalog.findVersion("pulsar").get().requiredVersion

// ── Apache RAT (Release Audit Tool) ─────────────────────────────────────────
tasks.named<org.nosphere.apache.rat.RatTask>("rat").configure {
    // Honour .gitignore exclusions so RAT skips untracked/generated files.
    // Register .gitignore files as inputs so the task re-runs when they change.
    inputs.files(fileTree(rootDir) {
        include("**/.gitignore")
        exclude("**/build/**")
        exclude("**/.gradle/**")
    })
    // use crlf plugin's gitignore dsl
    gitignore(rootDir)
    // Apply additional RAT-specific exclusions from .ratignore.
    val ratignoreFile = rootDir.resolve(".ratignore")
    inputs.file(ratignoreFile)
    exclude(ratignoreFile.readLines().map { it.trim() }.filter { it.isNotBlank() && !it.startsWith("#") })
}

apply(from = "gradle/verify-test-groups.gradle.kts")


idea {
    project {
        settings {
            // add ASL2 copyright profile to IntelliJ
            copyright {
                useDefault = "ASL2"
                profiles {
                    create("ASL2") {
                        notice = rootProject.file("src/license-header.txt").readText().trimEnd()
                        keyword = "Copyright"
                    }
                }
            }
        }
    }
}

// ── Root lifecycle tasks ────────────────────────────────────────────────────

tasks.register("serverDistTar") {
    dependsOn(":distribution:pulsar-server-distribution:serverDistTar")
}

tasks.register("docker") {
    description = "Build the Pulsar Docker image"
    group = "docker"
    dependsOn(":docker:pulsar-docker-image:dockerBuild")
}