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

// Convention plugin for building NAR (NiFi Archive) packages.
// NAR is essentially a ZIP with a specific directory structure:
//   META-INF/
//     MANIFEST.MF (with Nar-Id, Nar-Group, Nar-Version, etc.)
//     bundled-dependencies/
//       *.jar (all runtime dependencies + project jar)

plugins {
    java
}

// Generate NAR manifest as a separate task
val generateNarManifest = tasks.register("generateNarManifest") {
    val outputDir = layout.buildDirectory.dir("nar-manifest/META-INF")
    outputs.dir(outputDir)
    doLast {
        val dir = outputDir.get().asFile
        dir.mkdirs()
        dir.resolve("MANIFEST.MF").writeText(buildString {
            appendLine("Manifest-Version: 1.0")
            appendLine("Nar-Id: ${project.name}")
            appendLine("Nar-Group: ${project.group}")
            appendLine("Nar-Version: ${project.version}")
            appendLine("Build-Jdk: ${System.getProperty("java.version")}")
            appendLine("Built-By: Apache Pulsar")
        })
    }
}

// Ensure NAR waits for all upstream shadow JARs
afterEvaluate {
    configurations.getByName("runtimeClasspath").allDependencies
        .withType<ProjectDependency>()
        .forEach { dep ->
            dep.dependencyProject.tasks.findByName("shadowJar")?.let { shadowTask ->
                tasks.named("nar").configure { dependsOn(shadowTask) }
            }
        }
}

val narTask = tasks.register<Zip>("nar") {
    description = "Creates a NAR archive"
    group = "build"

    dependsOn(generateNarManifest)

    archiveExtension.set("nar")
    archiveBaseName.set("${project.name}-${project.version}")

    // Include generated manifest
    from(layout.buildDirectory.dir("nar-manifest")) {
        include("META-INF/MANIFEST.MF")
    }

    // Include the project's own JAR in bundled-dependencies
    from(tasks.named("jar")) {
        into("META-INF/bundled-dependencies")
    }

    // Include all runtime dependencies in bundled-dependencies
    from(configurations.runtimeClasspath) {
        into("META-INF/bundled-dependencies")
    }

    // Ensure reproducible builds
    isReproducibleFileOrder = true
    isPreserveFileTimestamps = false
}

tasks.named("build") {
    dependsOn(narTask)
}

// Register the NAR as a publishable artifact
artifacts {
    add("archives", narTask)
}
