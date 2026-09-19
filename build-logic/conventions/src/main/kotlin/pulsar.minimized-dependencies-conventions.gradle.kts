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

// Imported explicitly: the `java` plugin contributes a `java { }` extension accessor,
// so an unqualified `java.util.zip.ZipFile` would resolve `java` to that extension.
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import java.util.zip.ZipFile

// Convention for "<library> minimized" packaging modules. Produces a shadow jar that
// contains only the classes of the minimized libraries that are actually reachable from
// the module's reachability roots and their transitive closure.
//
// A consuming module:
//   * applies this plugin,
//   * declares its reachability roots as `api(project(...))` dependencies (e.g. the
//     pulsar projects that use the libraries being minimized), and
//   * configures `minimizedJar { minimizedDependencies.set(listOf("group:name")); maxRetainedClasses.set(N) }`.
//
// Why `api`: Shadow's minimize() seeds its reachability analysis (UnusedTracker) from
// the project's own source classes plus its `api`-scoped jars. A packaging module has no
// source, so the `api` roots are what drive the analysis; with `implementation` (or no
// roots) minimize() has nothing to start from and keeps the whole jar.

plugins {
    id("pulsar.java-conventions")
    id("pulsar.shadow-conventions")
}

val minimized = extensions.create<MinimizedDependenciesExtension>("minimizedJar")

// The `api` roots are a build-only reachability seed; strip them (and everything else)
// from the consumable variants so this module ships a self-contained jar with no
// transitive dependencies on consumers' classpaths.
listOf("apiElements", "runtimeElements").forEach { variant ->
    configurations.named(variant) {
        setExtendsFrom(emptySet())
    }
}

tasks.named<ShadowJar>("shadowJar") {
    val minimizedDeps = minimized.minimizedDependencies
    inputs.property("minimizedDependencies", minimizedDeps)
    // Bundle ONLY the minimized libraries; the `api` roots are read by minimize() as
    // reachability roots but are not part of the output jar.
    dependencies {
        minimizedDeps.get().forEach { coords -> include(dependency("$coords:.*")) }
    }
    // Drop every bundled class not reachable from the reachability roots.
    minimize()
}

// Verify the jar was actually pruned: the reachable set is small, so a count well above
// it but far below the full library jar catches a minimize() regression (e.g. the no-op
// that ships the whole jar). Stays configuration-cache compatible — the action captures
// only Providers, never the Project.
val verifyMinimizedJar = tasks.register("verifyMinimizedJar") {
    val jarFile = tasks.named<ShadowJar>("shadowJar").flatMap { it.archiveFile }
    val maxClasses = minimized.maxRetainedClasses
    inputs.file(jarFile)
    inputs.property("maxRetainedClasses", maxClasses)
    doLast {
        val limit = maxClasses.get()
        val classCount = ZipFile(jarFile.get().asFile).use { zf ->
            zf.entries().asSequence().count { it.name.endsWith(".class") }
        }
        if (classCount > limit) {
            throw GradleException(
                "Minimized jar retained $classCount classes (> $limit) — minimize() is not pruning."
            )
        }
        logger.lifecycle("Minimized jar OK: $classCount classes retained (limit $limit).")
    }
}

tasks.named("check") {
    dependsOn(verifyMinimizedJar)
}
