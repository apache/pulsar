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
    mergeServiceFiles()
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
