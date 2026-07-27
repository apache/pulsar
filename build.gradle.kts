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
import org.gradle.api.artifacts.ProjectDependency
import org.jetbrains.gradle.ext.copyright
import org.jetbrains.gradle.ext.settings

plugins {
    alias(libs.plugins.rat)
    alias(libs.plugins.version.catalog.update)
    alias(libs.plugins.versions)
    alias(libs.plugins.crlf) apply false
    alias(libs.plugins.idea.ext)
    alias(libs.plugins.spotless) apply false // workaround for https://github.com/diffplug/spotless/issues/2877
    // Publish repositories (local + ASF Nexus), signing, upload lock and snapshot/release validation
    // for the org.apache.pulsar:pulsar parent POM published below — shared with every module.
    id("pulsar.publish-repositories-conventions")
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

tasks.register("checkBinaryLicense") {
    group = "verification"
    description = "Check LICENSE/NOTICE coverage of bundled jars in all binary distributions"
    dependsOn(
        ":distribution:pulsar-server-distribution:checkBinaryLicense",
        ":distribution:pulsar-shell-distribution:checkBinaryLicense",
    )
}

tasks.register("docker") {
    description = "Build the Pulsar Docker image"
    group = "docker"
    dependsOn(":docker:pulsar-docker-image:dockerBuild")
}

// ── Aggregate verification tasks (quickCheck / sanityCheck) ───────────────────
// quickCheck   — fast, source-only conformance across every module: checkstyle + spotless + Apache
//                RAT. Compiles nothing, so it never builds shadow jars.
// sanityCheck  — pre-PR gate: quickCheck plus compiling main + test sources of every module that
//                does NOT pull a shaded artifact onto its compile classpath. Compiling such a module
//                would build a (slow) shadow jar, so those modules are skipped for compilation; their
//                sources are still covered by the checkstyle/spotless part of quickCheck.
//
// Compiling a module builds a shadow jar only when it declares a shaded module (one applying the
// shadow plugin) on its compile or test-compile classpath — compilation then needs that dependency's
// relocated jar. The shade convention plugins (pulsar.shadow-conventions, pulsar.client-shade-conventions)
// all apply `com.gradleup.shadow`, so the plugin id is the reliable marker for a shaded module.
// A shaded module's OWN compileJava/compileTestJava does NOT build its shadow jar, so the shaded and
// *-minimized modules are themselves compiled here; only their shadow-consuming dependents are skipped.
val shadowPluginId = "com.gradleup.shadow"
val compileScopeConfigurations = setOf(
    "api", "implementation", "compileOnly", "testImplementation", "testCompileOnly",
)
fun Project.dependsOnShadowJar(): Boolean =
    configurations
        .filter { it.name in compileScopeConfigurations }
        .any { configuration ->
            configuration.dependencies.withType(ProjectDependency::class.java).any { dependency ->
                rootProject.project(dependency.path).plugins.hasPlugin(shadowPluginId)
            }
        }

// The cross-project wiring is deferred to `provider {}` so it only runs when these aggregates are
// actually in the task graph, keeping configuration-on-demand intact for every other build.
tasks.register("quickCheck") {
    group = "verification"
    description = "Fast source-code conformance check across all modules (checkstyle + spotless + " +
        "Apache RAT). Compiles nothing and never builds shadow jars."
    dependsOn("rat")
    dependsOn(provider {
        subprojects.flatMap { sub ->
            listOf("checkstyleMain", "checkstyleTest", "spotlessCheck").mapNotNull { sub.tasks.findByName(it) }
        }
    })
}

tasks.register("sanityCheck") {
    group = "verification"
    description = "Pre-PR check: quickCheck plus compiling main + test sources of every module. " +
        "Modules that depend on shaded artifacts are skipped for compilation so no shadow jar is built."
    dependsOn("quickCheck")
    dependsOn(provider {
        subprojects
            .filter { it.plugins.hasPlugin("java") && !it.dependsOnShadowJar() }
            .flatMap { sub ->
                listOf("compileJava", "compileTestJava").mapNotNull { sub.tasks.findByName(it) }
            }
    })
}

// ── Parent POM publication ──────────────────────────────────────────────────
// Publishes org.apache.pulsar:pulsar as a POM-only parent artifact.
// Child modules reference this via <parent> in their POMs, inheriting
// shared ASF metadata (license, SCM, organization, etc.).
publishing {
    publications {
        create<MavenPublication>("maven") {
            pom {
                packaging = "pom"
                name.set("Apache Pulsar")
                description.set(
                    "Pulsar is a distributed pub-sub messaging platform with a very " +
                        "flexible messaging model and an intuitive client API."
                )
                url.set("https://pulsar.apache.org")
                inceptionYear.set("2017")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }

                organization {
                    name.set("Apache Software Foundation")
                    url.set("https://www.apache.org/")
                }

                issueManagement {
                    system.set("GitHub Issues")
                    url.set("https://github.com/apache/pulsar/issues")
                }

                scm {
                    connection.set("scm:git:https://github.com/apache/pulsar.git")
                    developerConnection.set("scm:git:https://github.com/apache/pulsar.git")
                    url.set("https://github.com/apache/pulsar")
                    tag.set("HEAD")
                }

                mailingLists {
                    mailingList {
                        name.set("Apache Pulsar developers list")
                        subscribe.set("dev-subscribe@pulsar.apache.org")
                        unsubscribe.set("dev-unsubscribe@pulsar.apache.org")
                        post.set("dev@pulsar.apache.org")
                        archive.set("https://lists.apache.org/list.html?dev@pulsar.apache.org")
                    }
                }

                developers {
                    developer {
                        organization.set("Apache Pulsar developers")
                        organizationUrl.set("https://pulsar.apache.org/")
                    }
                }
            }
        }
    }
}
