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

plugins {
    checkstyle
    id("com.diffplug.spotless")
}

val catalog = the<VersionCatalogsExtension>().named("libs")

// ── Checkstyle ──────────────────────────────────────────────────────────────

configure<CheckstyleExtension> {
    toolVersion = catalog.findVersion("checkstyle").get().requiredVersion
    configFile = rootProject.file("buildtools/src/main/resources/pulsar/checkstyle.xml")
    configProperties["checkstyle.suppressions.file"] =
        rootProject.file("buildtools/src/main/resources/pulsar/suppressions.xml").absolutePath
}

tasks.withType<Checkstyle>().configureEach {
    // Checkstyle here analyzes source only; none of the configured checks need type
    // resolution from compiled bytecode. By default Gradle wires the Checkstyle task
    // classpath to `sourceSet.output + sourceSet.compileClasspath`, which forces this
    // module (and every project dependency, including slow shaded/shadow jars) to be
    // compiled and assembled just to run checkstyle. Clearing it lets checkstyle run
    // directly on the sources without compiling anything.
    classpath = files()
    // Broker module has very large files that need more heap
    maxHeapSize.set("1g")
    // Exclude generated source files (proto, lightproto, etc.)
    exclude { it.file.path.contains("/build/") }
    exclude { it.file.path.contains("/generated-lightproto/") }
    exclude { it.file.path.contains("/generated-sources/") }
    // Match Maven exclusion: **/proto/*
    exclude("**/proto/*")
}

// Markers identifying generated source roots (protobuf/lightproto/avro output, all under build/).
// Files in these roots are already excluded from analysis above.
val generatedSourceRootMarkers = listOf("/build/", "/generated-lightproto/", "/generated-sources/")

// Run checkstyle straight from the hand-written source roots. Gradle wires the Checkstyle task's
// source to `sourceSet.allJava`, which includes the generated-source roots registered by the
// protobuf/lightproto/avro plugins. That makes checkstyle depend on those generator tasks, which in
// turn resolve proto classpaths that compile dependency projects (and build shaded jars) just to run
// checkstyle. Since the generated files are excluded from analysis anyway, restrict the source roots
// to the non-generated ones so checkstyle depends on nothing but the sources it actually checks.
plugins.withType<JavaBasePlugin>().configureEach {
    the<SourceSetContainer>().configureEach {
        val sourceSet = this
        tasks.matching { it.name == sourceSet.getTaskName("checkstyle", null) }
            .withType<Checkstyle>()
            .configureEach {
                val staticRoots = sourceSet.java.srcDirs.filter { dir ->
                    generatedSourceRootMarkers.none { dir.invariantSeparatorsPath.contains(it) }
                }
                setSource(files(staticRoots).asFileTree.matching { include("**/*.java") })
            }
    }
}

// ── License header check (Spotless) ────────────────────────────────────────
val asfLicenseHeader = rootProject.file("src/license-header.txt").readText()
val asfLicenseHeaderJava = "/*\n" + asfLicenseHeader.lines()
    .map { " * $it".trimEnd() }
    .joinToString("\n") + "/\n"
val asfLicenseHeaderJavadoc = asfLicenseHeaderJava.replaceFirst("/*", "/**")
configure<com.diffplug.gradle.spotless.SpotlessExtension> {
    java {
        // Only the hand-written main and test sources. Spotless's default Java target is every source
        // set's `allJava`, which includes the generated-proto roots (protobuf/lightproto/avro) and so
        // makes spotlessJava depend on those generators — dragging proto generation and dependency
        // compilation into what should be a source-only check. Targeting the static main/test roots
        // keeps spotless (and the quickCheck/sanityCheck aggregates) from compiling anything. Sources
        // outside src/main/java and src/test/java are still license-checked by the Apache RAT (`rat`) task.
        target("src/main/java/**/*.java", "src/test/java/**/*.java")
        targetExclude("**/AbstractCASReferenceCounted.java")
        licenseHeader(asfLicenseHeaderJava, "(\\n|package|import|public|class|module) ?")
    }

    format("proto") {
        target("src/*/proto/**/*.proto")
        licenseHeader(asfLicenseHeaderJavadoc, "\\n|syntax")
    }
}
