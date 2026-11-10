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
import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import com.github.vlsi.gradle.git.dsl.gitignore
import org.jetbrains.gradle.ext.copyright
import org.jetbrains.gradle.ext.settings
import org.nosphere.apache.rat.RatTask

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

tasks.named<DependencyUpdatesTask>("dependencyUpdates") {
    outputFormatter = "html"
    rejectVersionIf {
        val nonStable = candidate.version.contains("alpha") || candidate.version.contains("beta") || candidate.version.contains("rc")
        // OpenTelemetry publishes stable releases with -alpha suffix for some modules
        val isOpenTelemetry = candidate.group.startsWith("io.opentelemetry")
        nonStable && !(isOpenTelemetry && candidate.version.contains("alpha"))
    }
}

// ── Apache RAT (Release Audit Tool) ─────────────────────────────────────────
tasks.named<RatTask>("rat") {
    // Honour .gitignore exclusions so RAT skips untracked/generated files, using the crlf plugin's
    // gitignore DSL. The .gitignore files need no input of their own: they are read when the
    // configuration cache entry is stored, so a change to one invalidates the entry, and RAT's input
    // files are the filtered tree. (A `fileTree { include("**/.gitignore") }` input would hold no files
    // at all, since Gradle's default excludes drop them, but it would record every directory of the
    // checkout, ignored ones such as the git worktrees of coding agents included. RAT would then rerun
    // whenever a directory appeared anywhere, and walking those trees made the up-to-date check slow.)
    gitignore(rootDir)
    // Apply additional RAT-specific exclusions from .ratignore.
    val ratignoreFile = rootDir.resolve(".ratignore")
    inputs.file(ratignoreFile)
    exclude(ratignoreFile.readLines().map { it.trim() }.filter { it.isNotBlank() && !it.startsWith("#") })
}

// Reports the TestNG group assignments of the modules' test classes, see VerifyTestGroups.
//   ./gradlew verifyTestGroups
//   ./gradlew verifyTestGroups -PverifyModule=pulsar-broker
tasks.register<VerifyTestGroups>("verifyTestGroups") {
    description = "Verify that all test classes have TestNG group assignments and detect duplicates across CI groups"
    group = "verification"
    val filterModule = providers.gradleProperty("verifyModule").orNull
    // Only the modules' names and directories, which `isolated` gives without accessing their configuration
    testSourceDirs.set(subprojects.map { it.isolated }
        .filter { filterModule == null || it.name == filterModule }
        .associate { it.name to it.projectDirectory.dir("src/test/java").asFile.absolutePath })
}

idea {
    project {
        settings {
            // add ASL2 copyright profile to IntelliJ
            copyright {
                useDefault = "ASL2"
                profiles {
                    create("ASL2") {
                        notice = file("src/license-header.txt").readText().trimEnd()
                        keyword = "Copyright"
                    }
                }
            }
        }

    }
}

// ── Root lifecycle tasks ────────────────────────────────────────────────────

tasks.register("serverDistTar") {
    group = "distribution"
    description = "Build the Pulsar server binary distribution tarball"
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

tasks.register("updateBinaryLicense") {
    group = "build"
    description = "Update the jar versions in the LICENSE/NOTICE files of all binary distributions after a dependency upgrade"
    dependsOn(
        ":distribution:pulsar-server-distribution:updateBinaryLicense",
        ":distribution:pulsar-shell-distribution:updateBinaryLicense",
    )
}

// ── OpenTelemetry version alignment ────────────────────────────────────────
// OpenTelemetry releases each *-alpha artifact together with its stable line, at the same version
// plus "-alpha". The version catalog holds the two as separate versions, which Dependabot could
// update independently, so fail fast when they drift apart.
val versionCatalog = extensions.getByType<VersionCatalogsExtension>().named("libs")
for (versionLine in listOf("opentelemetry", "opentelemetry-instrumentation")) {
    val stableVersion = versionCatalog.findVersion(versionLine).get().requiredVersion
    val alphaVersion = versionCatalog.findVersion("$versionLine-alpha").get().requiredVersion
    if (alphaVersion != "$stableVersion-alpha") {
        throw GradleException(
            "gradle/libs.versions.toml: $versionLine-alpha = \"$alphaVersion\" must be " +
                "\"$stableVersion-alpha\" to match $versionLine = \"$stableVersion\"",
        )
    }
}

tasks.register("docker") {
    description = "Build the Pulsar Docker image"
    group = "docker"
    dependsOn(":docker:pulsar-docker-image:dockerBuild")
}

// ── Verification tasks (quickCheck / sanityCheck) ───────────────────────────
// Every Java module has its own quickCheck (checkstyle + spotless) and sanityCheck (quickCheck plus
// compiling main + test sources, unless that needs a shaded module's shadow jar), see
// pulsar.code-quality-conventions and pulsar.java-conventions. `./gradlew quickCheck` and
// `./gradlew sanityCheck` run them in every module and these in the root project, which add Apache
// RAT. Neither compiles anything that would build a shadow jar.
val quickCheck = tasks.register("quickCheck") {
    group = "verification"
    description = "Fast source-code conformance check: Apache RAT here, and checkstyle + spotless in each " +
        "module with `./gradlew quickCheck`. Compiles nothing and never builds shadow jars."
    dependsOn(tasks.named("rat"))
}

tasks.register("sanityCheck") {
    group = "verification"
    description = "Pre-PR check: quickCheck, and compiling main + test sources in each module with " +
        "`./gradlew sanityCheck`. Modules that depend on shaded artifacts are not compiled."
    dependsOn(quickCheck)
}

// ── Parent POM publication ──────────────────────────────────────────────────
// ── Flame graphs from JFR recordings ────────────────────────────────────────
// Converts the recordings produced by -PtestAsyncProfiler or by
// :tests:integration:profilingIntegrationTest into flame graphs. See CONTRIBUTING.md.
//   ./gradlew jfrFlamegraphs                       # every recording in build/test-profiles
//   ./gradlew jfrFlamegraphs -Pjfr=<file-or-dir>   # a specific recording, or another directory
tasks.register<JfrFlamegraphsTask>("jfrFlamegraphs") {
    group = "verification"
    description = "Convert JFR recordings into flame graphs next to them, in a <name>-flamegraphs " +
        "directory. Pass -Pjfr=<file-or-directory> to pick what to convert."

    val jfrInput = providers.gradleProperty("jfr")
        .map { file(it) }
        .orElse(layout.buildDirectory.dir("test-profiles").map { it.asFile })
    // A directory is wired in as a file tree rather than listed here: the listing has to happen when
    // the task runs, or a reused configuration cache entry would convert the recordings of the
    // previous profiling run and miss the one just made. The tree is searched recursively because
    // JMH's async-profiler integration writes its recording into a directory per benchmark.
    jfrFiles.from(jfrInput.map { if (it.isFile) files(it) else fileTree(it) { include("**/*.jfr") } })

    // async-profiler's converter, looked up the same way as the profiler library itself:
    // -Pjfrconv wins, then the bin/ directory of the async-profiler install that
    // LIBASYNCPROFILER_PATH points into, then the copy that Amazon Corretto ships in the JDK that
    // runs Gradle.
    jfrconvExecutable.set(
        providers.gradleProperty("jfrconv")
            .orElse(providers.environmentVariable("LIBASYNCPROFILER_PATH")
                .map { File(File(it).parentFile.parentFile, "bin/jfrconv").absolutePath })
            .orElse(providers.systemProperty("java.home").map { File(it, "bin/jfrconv").absolutePath })
            .map {
                if (!File(it).canExecute()) {
                    throw GradleException("async-profiler's jfrconv was not found at '$it'. Set " +
                        "-Pjfrconv=<path>, or LIBASYNCPROFILER_PATH to the profiler library of an " +
                        "async-profiler install. Amazon Corretto ships jfrconv in the JDK's bin " +
                        "directory.")
                }
                it
            })

    // A recording collects several events; render every one that is present in it.
    profileTypes.set(providers.gradleProperty("jfr.types")
        .map { it.split(",").map(String::trim).filter(String::isNotEmpty) }
        .getOrElse(listOf("cpu", "wall", "alloc", "lock")))

    // Converting is cheap, and the recordings change on every profiling run.
    outputs.upToDateWhen { false }
}

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
