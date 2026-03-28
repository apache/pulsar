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

buildscript {
    // The license plugin pulls in plexus-utils:2.0.6 which conflicts with
    // the Shadow plugin's plexus-utils:4.0.2 (missing 4-arg matchPath method).
    // Force the newer version to avoid NoSuchMethodError at shading time.
    configurations.classpath {
        resolutionStrategy.force("org.codehaus.plexus:plexus-utils:4.0.2")
    }
}

plugins {
    alias(libs.plugins.rat)
    alias(libs.plugins.license) apply false
}

val catalog = the<VersionCatalogsExtension>().named("libs")
val pulsarVersion = catalog.findVersion("pulsar").get().requiredVersion

apply(from = "gradle/code-quality.gradle.kts")

allprojects {
    group = "org.apache.pulsar"
    version = pulsarVersion
}

subprojects {
    // Platform modules use java-platform which is mutually exclusive with java-library.
    if (project.name == "pulsar-dependencies" || project.name == "pulsar-bom") {
        return@subprojects
    }

    apply(plugin = "java-library")

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        options.release.set(17)
        options.compilerArgs.addAll(listOf("-parameters"))
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
                useVersion(rootProject.libs.versions.jackson.get())
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
        "compileOnly"(rootProject.libs.lombok)
        "annotationProcessor"(rootProject.libs.lombok)
        "testCompileOnly"(rootProject.libs.lombok)
        "testAnnotationProcessor"(rootProject.libs.lombok)

        // Common test dependencies (from parent POM)
        if (project.name != "buildtools") {
            "testRuntimeOnly"(project(":buildtools"))
        }
        "testImplementation"(rootProject.libs.testng)
        "testImplementation"(rootProject.libs.mockito.core)
        "testImplementation"(rootProject.libs.assertj.core)
        "testImplementation"(rootProject.libs.awaitility)
        "testImplementation"(rootProject.libs.system.lambda)
        "testImplementation"(rootProject.libs.slf4j.api)
    }

    tasks.withType<Test> {
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
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.SHORT
            showStackTraces = true
            showExceptions = true
            showCauses = true
        }
        maxHeapSize = "1300m"
        maxParallelForks = (providers.gradleProperty("maxParallelForks").orNull?.toInt() ?: 4)
        systemProperty("testRetryCount", System.getProperty("testRetryCount", "1"))
        systemProperty("testFailFast", System.getProperty("testFailFast", "true"))
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

    // Add shared test certificates as a test resource directory for modules that need them.
    // These modules rely on the shared tests/certificate-authority directory for TLS test certs.
    // By adding it as a source set resource (under "certificate-authority/"), Gradle's
    // processTestResources handles the copy and all downstream tasks see it automatically.
    val modulesNeedingCerts = setOf(
        "pulsar-broker", "pulsar-broker-common", "pulsar-broker-auth-oidc",
        "pulsar-broker-auth-sasl", "pulsar-common", "pulsar-proxy",
        "bcfips-include-test", "pulsar-testclient",
    )
    if (project.name in modulesNeedingCerts) {
        project.the<SourceSetContainer>()["test"].resources.srcDir(
            rootProject.file("tests").absolutePath
        )
        // Some modules already have certificate-authority files in their own test resources,
        // creating duplicates with the shared directory above.
        tasks.named<ProcessResources>("processTestResources") {
            duplicatesStrategy = DuplicatesStrategy.INCLUDE
        }
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

    // NAR modules should not bundle Pulsar platform dependencies — they are provided
    // at runtime by Pulsar's classloader hierarchy.
    // Note: pulsar-io-common is NOT in java-instance.jar (runtime-all), so it must be
    // bundled in each NAR that uses it (e.g., IOConfigUtils).
    pluginManager.withPlugin("io.github.merlimat.nar") {
        val pulsarPlatformModules = setOf(
            "pulsar-client-api",
            "pulsar-client-admin-api",
            "pulsar-client-original",
            "pulsar-client",
            "pulsar-common",
            "pulsar-config-validation",
            "bouncy-castle-bc",
            "pulsar-functions-api",
            "pulsar-functions-instance",
            "pulsar-functions-proto",
            "pulsar-functions-secrets",
            "pulsar-functions-utils",
            "pulsar-io-core",
            "pulsar-metadata",
            "pulsar-opentelemetry",
            "managed-ledger",
            "pulsar-package-core",
        )
        configurations.named("runtimeClasspath") {
            exclude(group = "org.apache.bookkeeper")
            // Protobuf is in java-instance.jar (runtime-all), so NARs must not bundle it.
            // Bundling a different version causes GeneratedMessage.getUnknownFields() conflicts.
            exclude(group = "com.google.protobuf")
            pulsarPlatformModules.forEach { module ->
                exclude(group = "org.apache.pulsar", module = module)
            }
        }

        // The NAR plugin copies from runtimeClasspath which resolves project dependencies
        // as class directories, not JARs. The NarClassLoader expects JARs in
        // META-INF/bundled-dependencies/. Force the NAR task to use JAR artifacts.
        val jarView = configurations.named("runtimeClasspath").get()
            .incoming.artifactView {
                attributes {
                    attribute(
                        LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                        objects.named(LibraryElements::class.java, LibraryElements.JAR)
                    )
                }
            }.files
        tasks.named("nar", Jar::class.java) {
            into("META-INF/bundled-dependencies") {
                from(jarView)
                duplicatesStrategy = DuplicatesStrategy.EXCLUDE
            }
        }
    }

    // Set archive names to match Maven artifactId for nested modules.
    // Skip if the project name is already qualified (starts with parent name),
    // which happens for sub-modules that use qualified names in settings.gradle.kts
    // to avoid Gradle name clashes.
    val parentProject = project.parent
    if (parentProject != null && parentProject != rootProject && parentProject.parent != rootProject
            && !project.name.startsWith(parentProject.name)) {
        val qualifiedName = "${parentProject.name}-${project.name}"
        the<BasePluginExtension>().archivesName.set(qualifiedName)
        // Also set NAR plugin's narId if NAR plugin is applied
        pluginManager.withPlugin("io.github.merlimat.nar") {
            @Suppress("UNCHECKED_CAST")
            val narExt = extensions.getByName("nar")
            val narIdProp = narExt.javaClass.getMethod("getNarId").invoke(narExt) as Property<String>
            narIdProp.set(qualifiedName)
        }
    }

    tasks.withType<Jar> {
        manifest {
            attributes(
                "Implementation-Title" to project.name,
                "Implementation-Version" to project.version,
            )
        }
    }
}

apply(from = "gradle/verify-test-groups.gradle.kts")

tasks.register("serverDistTar") {
    dependsOn(":distribution:pulsar-server-distribution:serverDistTar")
}

tasks.register("docker") {
    description = "Build the Pulsar Docker image"
    group = "docker"
    dependsOn(":docker:pulsar-docker-image:dockerBuild")
}

// Access version catalog from subprojects
val Project.libs: org.gradle.accessors.dm.LibrariesForLibs
    get() = rootProject.extensions.getByType()

// Filtered bookkeeper-server test-jar that excludes classes conflicting with testmocks
// (BookKeeperTestClient and TestStatsProvider have Pulsar-specific versions in testmocks).
// Exposed as a consumable configuration so consuming projects can depend on it via:
//   testImplementation(project(path = ":", configuration = "filteredBkServerTestJar"))
val bkServerTestJarResolvable by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    isTransitive = false
}
dependencies {
    bkServerTestJarResolvable(libs.bookkeeper.server) { artifact { classifier = "tests" } }
}
val filteredBkServerTestJarTask = tasks.register<Jar>("filteredBkServerTestJarTask") {
    archiveFileName.set("bookkeeper-server-tests-filtered.jar")
    destinationDirectory.set(layout.buildDirectory.dir("libs"))
    from(zipTree(bkServerTestJarResolvable.singleFile)) {
        exclude("org/apache/bookkeeper/client/BookKeeperTestClient*")
        exclude("org/apache/bookkeeper/client/TestStatsProvider*")
    }
}
configurations.consumable("filteredBkServerTestJar")
artifacts.add("filteredBkServerTestJar", filteredBkServerTestJarTask)
