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

// Convention plugin for modules using the Shadow plugin.
// Applies the shadow plugin, disables the default jar task, and makes the
// shadow jar the primary artifact for both runtimeElements and apiElements,
// so plain project() dependencies resolve to the shadow jar.

plugins {
    id("com.gradleup.shadow")
}

shadow {
    addShadowVariantIntoJavaComponent.set(false)
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveClassifier.set("")
    // ShadowJar's duplicatesStrategy is applied *before* the resource transformers run, so its
    // EXCLUDE default drops every duplicate META-INF/services file and mergeServiceFiles() ends up
    // keeping only the first provider file instead of merging them all. Keep EXCLUDE as the global
    // strategy — class files bypass the transformers entirely (ShadowCopyAction handles them in a
    // dedicated branch), so the strategy is the only thing deduplicating them — and let just the
    // service descriptors through to the transformer.
    mergeServiceFiles()
    // Paths owned by a resource transformer: ServiceFileTransformer (mergeServiceFiles) and the
    // KotlinModuleMetadataTransformer that ShadowJar applies on its own.
    val transformedPaths = listOf("META-INF/services/**", "META-INF/*.kotlin_module")
    filesMatching(transformedPaths) {
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }
    // filesMatching {} actions are not part of the task's input fingerprint, so without this the
    // build cache serves jars built under the old behaviour even after this block changes.
    inputs.property("transformedPathsDuplicatesStrategy", "$transformedPaths=INCLUDE")
    // Nothing should reach the archive twice; catch it if a future change makes it happen.
    failOnDuplicateEntries.set(true)
}

tasks.named<Jar>("jar") {
    enabled = false
}

configurations {
    named("runtimeElements") {
        outgoing {
            artifacts.clear()
            artifact(tasks.named("shadowJar"))
            variants.clear()
        }
    }
    named("apiElements") {
        outgoing {
            artifacts.clear()
            artifact(tasks.named("shadowJar"))
            variants.clear()
        }
    }
}
