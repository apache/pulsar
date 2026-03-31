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
    `java-library`
    id("pulsar.code-quality-conventions")
}

val catalog = the<VersionCatalogsExtension>().named("libs")

group = "org.apache.pulsar"
version = catalog.findVersion("pulsar").get().requiredVersion

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(17)
    options.compilerArgs.addAll(listOf("-parameters", "-Xlint:deprecation", "-Xlint:unchecked"))
}

configurations.all {
    // Exclude the old SLF4J 1.x bridge pulled in by bookkeeper-server.
    // Pulsar uses SLF4J 2.x with log4j-slf4j2-impl; having both causes
    // NoSuchMethodError in Log4jLoggerFactory at test startup.
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")

    // Force Jackson version to match the version catalog. Transitive dependencies
    // (e.g. from jackson-bom) can pull in newer versions that break API compatibility
    // (EnumResolver.constructUsingToString signature changed in 2.19+).
    resolutionStrategy.eachDependency {
        if (requested.group.startsWith("com.fasterxml.jackson")) {
            useVersion(catalog.findVersion("jackson").get().requiredVersion)
        }
    }
}

// Exclude bc-fips from modules that don't need it. bc-fips's CryptoServicesRegistrar
// conflicts with bcprov-jdk18on's version — having both causes NoSuchMethodError.
// Only the FIPS-specific modules and modules with explicit FIPS tests should have it.
val modulesUsingBcFips = setOf(
    "bcfips", "bcfips-include-test",
    "pulsar-common", "pulsar-broker-common",
)
if (project.name !in modulesUsingBcFips) {
    configurations.all {
        exclude(group = "org.bouncycastle", module = "bc-fips")
    }
}

dependencies {
    // Exclude all BouncyCastle from bookkeeper-server (matches Maven parent POM exclusion).
    // BookKeeper's bc-fips transitive dependency contains a CryptoServicesRegistrar that
    // conflicts with the non-FIPS version in bcprov-jdk18on. Pulsar manages its own BC deps.
    components {
        withModule("org.apache.bookkeeper:bookkeeper-server") {
            allVariants {
                withDependencies {
                    removeAll { it.group == "org.bouncycastle" }
                }
            }
        }
    }

    // Enforced platform pins all dependency versions from the version catalog.
    // This is the Gradle equivalent of Maven's dependencyManagement section.
    "implementation"(enforcedPlatform(project(":pulsar-dependencies")))

    // Resolve lz4-java capability conflict: at.yawk.lz4:lz4-java (used by Pulsar) and
    // org.lz4:lz4-java (used by kafka-clients) both provide the org.lz4:lz4-java capability.
    // Prefer at.yawk.lz4 which is the version Pulsar standardizes on.
    configurations.all {
        resolutionStrategy.capabilitiesResolution.withCapability("org.lz4:lz4-java") {
            select("at.yawk.lz4:lz4-java:0")
        }
    }

    // Allow overriding protobuf version via -PprotobufVersion=4.31.1 for protobuf v4 tests
    providers.gradleProperty("protobufVersion").orNull?.let { protobufVersion ->
        configurations.all {
            resolutionStrategy {
                force("com.google.protobuf:protobuf-java:$protobufVersion")
            }
        }
    }

    // Annotation processing for Lombok
    "compileOnly"(catalog.findLibrary("lombok").get())
    "annotationProcessor"(catalog.findLibrary("lombok").get())
    "testCompileOnly"(catalog.findLibrary("lombok").get())
    "testAnnotationProcessor"(catalog.findLibrary("lombok").get())

    // Common test dependencies (from parent POM)
    if (project.name != "buildtools") {
        "testRuntimeOnly"(project(":buildtools"))
    }
    "testImplementation"(catalog.findLibrary("testng").get())
    "testImplementation"(catalog.findLibrary("mockito-core").get())
    "testImplementation"(catalog.findLibrary("assertj-core").get())
    "testImplementation"(catalog.findLibrary("awaitility").get())
    "testImplementation"(catalog.findLibrary("system-lambda").get())
    "testImplementation"(catalog.findLibrary("slf4j-api").get())
}

tasks.withType<Test>().configureEach {
    useTestNG {
        listeners.addAll(listOf(
            "org.apache.pulsar.tests.PulsarTestListener",
            "org.apache.pulsar.tests.AnnotationListener",
            "org.apache.pulsar.tests.FailFastNotifier",
            "org.apache.pulsar.tests.MockitoCleanupListener",
            "org.apache.pulsar.tests.FastThreadLocalCleanupListener",
            "org.apache.pulsar.tests.ThreadLeakDetectorListener",
            "org.apache.pulsar.tests.SingletonCleanerListener",
        ))
        // TestNG group filtering: -PtestGroups=broker,broker-admin -PexcludedTestGroups=flaky
        providers.gradleProperty("testGroups").orNull?.let { groups ->
            includeGroups(*groups.split(",").map { it.trim() }.toTypedArray())
        }
        val excludedTestGroups = providers.gradleProperty("excludedTestGroups").getOrElse("quarantine,flaky")
        excludeGroups(*(excludedTestGroups.split(",").map { it.trim() }.toTypedArray()))
    }
    testLogging {
        events("FAILED")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStackTraces = true
        showExceptions = true
        showCauses = true
    }
    maxHeapSize = "1300m"
    maxParallelForks = 4
    val failFastValue = providers.gradleProperty("testFailFast").getOrElse("true").toBoolean()
    failFast = failFastValue
    systemProperty("testRetryCount", providers.gradleProperty("testRetryCount").getOrElse("1"))
    systemProperty("testFailFast", failFastValue.toString())
    jvmArgs(
        "--add-opens", "java.base/jdk.internal.loader=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.io=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
        "--add-opens", "java.base/sun.net=ALL-UNNAMED",
        "--add-opens", "java.management/sun.management=ALL-UNNAMED",
        "--add-opens", "jdk.management/com.sun.management.internal=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.platform=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "-XX:+EnableDynamicAgentLoading",
        "-Xshare:off",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dpulsar.allocator.pooled=true",
        "-Dpulsar.allocator.exit_on_oom=false",
        "-Dpulsar.allocator.out_of_memory_policy=FallbackToHeap",
        "-Dpulsar.test.preventExit=true",
    )
}

// Expose test classes for cross-module test dependencies (Maven test-jar equivalent)
val testJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(project.the<SourceSetContainer>()["test"].output)
}

configurations.create("testJar") {
    isCanBeConsumed = true
    isCanBeResolved = false
    extendsFrom(configurations["testImplementation"], configurations["testRuntimeOnly"])
}
artifacts.add("testJar", testJar)

// Set archive names to match Maven artifactId for nested modules.
// Skip if the project name is already qualified (starts with parent name),
// which happens for sub-modules that use qualified names in settings.gradle.kts
// to avoid Gradle name clashes.
val parentProject = project.parent
if (parentProject != null && parentProject != rootProject && parentProject.parent != rootProject
        && !project.name.startsWith(parentProject.name)) {
    the<BasePluginExtension>().archivesName.set("${parentProject.name}-${project.name}")
}

tasks.withType<Jar>().configureEach {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
        )
    }
}

// Add a task for viewing all configurations for all projects in a simple way
tasks.register<DependencyReportTask>("allDependencies"){}