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
import org.gradle.api.tasks.PathSensitivity
import java.util.zip.ZipFile

// Convention plugin for Pulsar client shaded modules (pulsar-client-shaded,
// pulsar-client-admin-shaded, pulsar-client-all). Configures the shadow jar
// with the shared dependency includes, file excludes, relocations, and
// filesMatching blocks. Modules only need to define their project dependencies.

plugins {
    id("pulsar.shadow-conventions")
}

val shadePrefix = "org.apache.pulsar.shade"
extra["shadePrefix"] = shadePrefix

// ---- Published dependency scopes for non-bundled dependencies ----
// The Shadow plugin publishes the `shadow` configuration's dependencies as the dependency-reduced
// POM/Gradle Module Metadata of the shaded artifact, mapping ALL of them to Maven `runtime` scope
// (a single java-runtime variant). That loses the api/implementation distinction the original
// modules express, so consumers can't compile against the parts of the API that live in non-bundled
// modules (e.g. pulsar-client-api).
//
// To restore control, modules can declare a dependency in the `shadowApi` configuration instead of
// `shadow`. `shadowApi` deps are published with `compile` scope (a java-api variant added to the
// shadow component), while `shadow` deps stay `runtime`. Both end up in the POM and GMM:
//   "shadowApi"(...)  -> compile scope  (consumer compile + runtime classpath)
//   "shadow"(...)     -> runtime scope  (consumer runtime classpath only)
val shadowApi = configurations.dependencyScope("shadowApi") {
    description = "Non-bundled dependencies published with compile (api) scope in the shaded " +
        "artifact's dependency-reduced POM and Gradle Module Metadata."
}
val shadowApiElements = configurations.consumable("shadowApiElements") {
    description = "API elements (compile scope) of the shaded artifact, mirroring shadowRuntimeElements."
    extendsFrom(shadowApi.get())
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_API))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.SHADOWED))
    }
    // Carry the shaded jar so this variant is a complete API variant (like apiElements does for the
    // standard java-library component).
    outgoing.artifact(tasks.named("shadowJar"))
}
// Add the java-api variant to the shadow component so maven-publish emits the shadowApi deps with
// compile scope. The shadow plugin already wires shadowRuntimeElements -> runtime scope.
(components["shadow"] as AdhocComponentWithVariants)
    .addVariantsFromConfiguration(shadowApiElements.get()) { mapToMavenScope("compile") }

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {

    // ---- Dependency includes ----
    dependencies {
        include(project(":pulsar-client-original"))
        include(project(":pulsar-client-admin-original"))
        include(project(":pulsar-common"))
        include(project(":pulsar-client-messagecrypto-bc"))
        include(project(":pulsar-client-fastutil-minimized"))
        include(dependency("com.fasterxml.jackson.*:.*"))
        include(dependency("com.google.*:.*"))
        include(dependency("com.google.auth:.*"))
        include(dependency("com.google.code.findbugs:.*"))
        include(dependency("com.google.code.gson:gson"))
        include(dependency("com.google.errorprone:.*"))
        include(dependency("com.google.guava:.*"))
        include(dependency("com.google.j2objc:.*"))
        include(dependency("com.google.re2j:re2j"))
        include(dependency("com.spotify:completable-futures"))
        include(dependency("com.squareup.*:.*"))
        include(dependency("org.eclipse.angus:angus-activation"))
        include(dependency("com.thoughtworks.paranamer:paranamer"))
        include(dependency("com.typesafe.netty:netty-reactive-streams"))
        include(dependency("org.apache.datasketches:.*"))
        include(dependency("commons-.*:.*"))
        include(dependency("io.airlift:.*"))
        include(dependency("io.grpc:.*"))
        include(dependency("io.netty:.*"))
        include(dependency("io.opencensus:.*"))
        include(dependency("io.perfmark:.*"))
        include(dependency("io.prometheus:.*"))
        include(dependency("io.swagger.core.v3:.*"))
        include(dependency("jakarta.activation:jakarta.activation-api"))
        include(dependency("jakarta.annotation:jakarta.annotation-api"))
        include(dependency("jakarta.inject:jakarta.inject-api"))
        include(dependency("jakarta.ws.rs:jakarta.ws.rs-api"))
        include(dependency("jakarta.xml.bind:jakarta.xml.bind-api"))
        include(dependency("org.apache.avro:.*"))
        include(dependency("org.apache.bookkeeper:.*"))
        include(dependency("org.apache.commons:commons-compress"))
        include(dependency("org.apache.commons:commons-lang3"))
        include(dependency("org.asynchttpclient:.*"))
        include(dependency("org.checkerframework:.*"))
        include(dependency("org.eclipse.jetty:.*"))
        include(dependency("org.glassfish.hk2.*:.*"))
        include(dependency("org.glassfish.jersey.*:.*"))
        include(dependency("org.javassist:javassist"))
        include(dependency("org.jvnet.mimepull:.*"))
        include(dependency("org.objenesis:.*"))
        include(dependency("org.reactivestreams:reactive-streams"))
        include(dependency("org.tukaani:xz"))
        include(dependency("org.yaml:snakeyaml"))
        include(dependency("org.roaringbitmap:RoaringBitmap"))
        include(dependency("com.github.ben-manes.caffeine:.*"))
        exclude(dependency("com.fasterxml.jackson.core:jackson-annotations"))
        exclude(dependency("com.google.protobuf:protobuf-java"))
        exclude(dependency("io.netty:netty-transport-native-kqueue"))
    }

    // ---- File excludes ----
    // Exclude bouncycastle (signatures would break if shaded)
    exclude("org/bouncycastle/**")
    exclude("**/module-info.class")
    exclude("findbugsExclude.xml")
    exclude("META-INF/*-LICENSE")
    exclude("META-INF/*-NOTICE")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    exclude("META-INF/*.SF")
    exclude("META-INF/DEPENDENCIES*")
    exclude("META-INF/io.netty.versions.properties")
    exclude("META-INF/LICENSE*")
    exclude("META-INF/license/**")
    exclude("META-INF/maven/**")
    exclude("META-INF/native-image/**")
    exclude("META-INF/NOTICE*")
    exclude("META-INF/proguard/**")

    // ---- Relocations ----
    relocate("com.fasterxml.jackson", "$shadePrefix.com.fasterxml.jackson") {
        exclude("com.fasterxml.jackson.annotation.*")
    }
    relocate("com.google", "$shadePrefix.com.google") {
        exclude("com.google.protobuf.**")
    }
    relocate("org.apache.avro", "$shadePrefix.org.apache.avro") {
        exclude("org.apache.avro.reflect.AvroAlias")
        exclude("org.apache.avro.reflect.AvroDefault")
        exclude("org.apache.avro.reflect.AvroEncode")
        exclude("org.apache.avro.reflect.AvroIgnore")
        exclude("org.apache.avro.reflect.AvroMeta")
        exclude("org.apache.avro.reflect.AvroName")
        exclude("org.apache.avro.reflect.AvroSchema")
        exclude("org.apache.avro.reflect.Nullable")
        exclude("org.apache.avro.reflect.Stringable")
        exclude("org.apache.avro.reflect.Union")
    }
    relocate("org.apache.pulsar.policies", "$shadePrefix.org.apache.pulsar.policies") {
        exclude("org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport")
        exclude("org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats")
        exclude("org.apache.pulsar.policies.data.loadbalancer.ResourceUsage")
        exclude("org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData")
    }
    relocateWithPrefix(shadePrefix, "com.github.benmanes")
    relocateWithPrefix(shadePrefix, "com.spotify.futures")
    relocateWithPrefix(shadePrefix, "com.squareup")
    relocateWithPrefix(shadePrefix, "org.eclipse.angus")
    relocateWithPrefix(shadePrefix, "com.thoughtworks.paranamer")
    relocateWithPrefix(shadePrefix, "com.typesafe")
    relocateWithPrefix(shadePrefix, "io.airlift")
    relocateWithPrefix(shadePrefix, "io.grpc")
    relocateWithPrefix(shadePrefix, "io.netty")
    relocateWithPrefix(shadePrefix, "io.opencensus")
    relocateWithPrefix(shadePrefix, "io.prometheus.client")
    relocateWithPrefix(shadePrefix, "io.swagger")
    relocateWithPrefix(shadePrefix, "it.unimi.dsi.fastutil")
    relocateWithPrefix(shadePrefix, "javassist")
    relocateWithPrefix(shadePrefix, "jakarta.activation")
    relocateWithPrefix(shadePrefix, "jakarta.annotation")
    relocateWithPrefix(shadePrefix, "jakarta.inject")
    relocateWithPrefix(shadePrefix, "jakarta.ws.rs")
    relocateWithPrefix(shadePrefix, "jakarta.xml.bind")
    relocateWithPrefix(shadePrefix, "jersey")
    relocateWithPrefix(shadePrefix, "okio")
    relocateWithPrefix(shadePrefix, "org.aopalliance")
    relocateWithPrefix(shadePrefix, "org.apache.bookkeeper")
    relocateWithPrefix(shadePrefix, "org.apache.commons")
    relocateWithPrefix(shadePrefix, "org.apache.datasketches")
    relocateWithPrefix(shadePrefix, "org.apache.pulsar.checksum")
    relocateWithPrefix(shadePrefix, "org.asynchttpclient")
    relocateWithPrefix(shadePrefix, "org.checkerframework")
    relocateWithPrefix(shadePrefix, "org.codehaus.jackson")
    relocateWithPrefix(shadePrefix, "org.eclipse.jetty")
    relocateWithPrefix(shadePrefix, "org.glassfish")
    relocateWithPrefix(shadePrefix, "org.jvnet")
    relocateWithPrefix(shadePrefix, "org.objenesis")
    relocateWithPrefix(shadePrefix, "org.reactivestreams")
    relocateWithPrefix(shadePrefix, "org.roaringbitmap")
    relocateWithPrefix(shadePrefix, "org.tukaani")
    relocateWithPrefix(shadePrefix, "org.yaml")
    // NOTE: Do NOT shade log4j, otherwise logging won't work

    // ---- File content transformations ----
    relocateAsyncHttpClientProperties(shadePrefix)
    // Relocate Netty native library filenames so the shaded Netty NativeLibraryLoader
    // can find them (and to avoid conflicts with unshaded Netty). The loader prepends
    // the shaded package prefix, with dots replaced by underscores, to the library
    // name, so the prefix has to be inserted right before "netty" (after the "lib"
    // prefix that .so/.jnilib files carry). For example
    //   META-INF/native/libnetty_transport_native_epoll_x86_64.so
    // must be renamed to
    //   META-INF/native/liborg_apache_pulsar_shade_netty_transport_native_epoll_x86_64.so
    val nettyNativePrefix = shadePrefix.replace('.', '_') + "_"
    filesMatching("META-INF/native/**") {
        if (name.matches(Regex("(lib)?netty.+\\.(so|jnilib|dll)"))) {
            path = path.replace(name, name.replaceFirst("netty", "${nettyNativePrefix}netty"))
        }
    }
}

// ---- Shaded jar content validation ----
// Safety net: after the shaded jar is built, fail the build if it contains any file outside the
// allowed package roots. This catches dependencies that ended up in the jar without being relocated
// (e.g. leaked javax.* classes) or native libraries whose filenames were not rewritten. Directory
// entries (the parent directories of the allowed roots) are ignored.
val allowedShadedRoots = listOf(
    "META-INF/",
    "org/apache/pulsar/",
    "com/scurrilous/circe/",
    // Avro reflection annotations are intentionally excluded from the org.apache.avro relocation so
    // that Avro recognizes them by their original name on user-supplied classes.
    "org/apache/avro/reflect/",
    // Bundled native libraries (libcirce-checksum.so, libcpu-affinity.so).
    "lib/",
)
val verifyShadedJarContents = tasks.register("verifyShadedJarContents") {
    description = "Fails the build if the shaded jar contains files outside the allowed package roots."
    group = "verification"
    val shadedJar = tasks.named<ShadowJar>("shadowJar").flatMap { it.archiveFile }
    inputs.file(shadedJar).withPropertyName("shadedJar").withPathSensitivity(PathSensitivity.NONE)
    inputs.property("allowedRoots", allowedShadedRoots)
    val report = layout.buildDirectory.file("verification/verifyShadedJarContents.txt")
    outputs.file(report).withPropertyName("report")
    val allowedRoots = allowedShadedRoots
    doLast {
        val jar = shadedJar.get().asFile
        val violations = mutableListOf<String>()
        ZipFile(jar).use { zip ->
            val entries = zip.entries()
            while (entries.hasMoreElements()) {
                val entry = entries.nextElement()
                if (entry.isDirectory) continue
                if (allowedRoots.none { entry.name.startsWith(it) }) {
                    violations.add(entry.name)
                }
            }
        }
        violations.sort()
        if (violations.isNotEmpty()) {
            throw GradleException(
                "Shaded jar ${jar.name} contains ${violations.size} file(s) outside the allowed roots " +
                    "$allowedRoots:\n" + violations.joinToString("\n") { "  $it" }
            )
        }
        report.get().asFile.run {
            parentFile.mkdirs()
            writeText(
                "OK: ${jar.name} contents are within the allowed roots:\n" +
                    allowedRoots.joinToString("\n") { "  $it" } + "\n"
            )
        }
    }
}
tasks.named<ShadowJar>("shadowJar") {
    finalizedBy(verifyShadedJarContents)
}
