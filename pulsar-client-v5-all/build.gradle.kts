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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.attributes.Bundling
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.component.AdhocComponentWithVariants

plugins {
    id("pulsar.public-java-library-conventions")
    id("pulsar.client-shade-conventions")
}

// Keep the ordinary artifact's graph unshaded. Only the optional Shadow jar uses minimized fastutil.
val externalRuntime = configurations.dependencyScope("externalRuntime")
val shadedImplementation = configurations.dependencyScope("shadedImplementation")
val shadedRuntimeClasspath = configurations.resolvable("shadedRuntimeClasspath") {
    extendsFrom(shadedImplementation.get(), configurations.named("internalPlatform").get())
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
    }
}

// These dependencies remain external for both choices. Keeping them in the ordinary POM also
// lets Maven classifier consumers retain them when excluding the bundled implementation modules.
configurations.api {
    extendsFrom(configurations.named("shadowApi").get())
}
configurations.runtimeOnly {
    extendsFrom(externalRuntime.get())
}
configurations.shadow {
    extendsFrom(externalRuntime.get())
}

dependencies {
    api(project(":pulsar-client-v5"))
    api(project(":pulsar-client-admin-original"))
    runtimeOnly(project(":pulsar-client-messagecrypto-bc"))

    shadedImplementation(project(":pulsar-client-v5")) {
        exclude(group = "it.unimi.dsi", module = "fastutil")
    }
    shadedImplementation(project(":pulsar-client-admin-original")) {
        exclude(group = "it.unimi.dsi", module = "fastutil")
    }
    shadedImplementation(project(":pulsar-client-fastutil-minimized"))
    shadedImplementation(project(":pulsar-client-messagecrypto-bc"))

    // API dependencies remain external in both variants. Runtime libraries are shared via
    // externalRuntime, avoiding inheritance of the jar artifact attached to Shadow's configuration.
    // protobuf-java remains opt-in for applications using protobuf schemas.
    "shadowApi"(project(":pulsar-client-api"))
    "shadowApi"(project(":pulsar-client-admin-api"))
    // PIP-478: see the note in pulsar-client-shaded — the bundled classes surface these three API
    // modules on their exported ABI, and a plugin author compiles against the same coordinates.
    "shadowApi"(project(":pulsar-client-api-v5"))
    "shadowApi"(project(":pulsar-tls-factory-api"))
    "shadowApi"(project(":pulsar-http-client-api"))
    "shadowApi"(libs.opentelemetry.api)
    externalRuntime(libs.jackson.annotations)
    externalRuntime(libs.bcprov.jdk18on)
    externalRuntime(libs.bcpkix.jdk18on)
    externalRuntime(libs.opentelemetry.api.incubator)
    externalRuntime(libs.slog)
    externalRuntime(libs.slf4j.api)
    externalRuntime(libs.jspecify)
}

// Shadow's default "all" classifier coexists with the ordinary jar. Publish both compile and
// runtime shaded variants so Gradle consumers select the jar and its external dependencies together.
(components["java"] as AdhocComponentWithVariants).addVariantsFromConfiguration(
    configurations.named("shadowApiElements").get()
) {
    mapToMavenScope("compile")
    mapToOptional()
}
tasks.named<ShadowJar>("shadowJar") {
    configurations.set(shadedRuntimeClasspath.map { setOf(it) })
}
