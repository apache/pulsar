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

// Versions for dependencies injected by the component-metadata rules below, captured as plain
// strings so the rule closures stay configuration-cache compatible (and so the injected deps carry
// explicit versions rather than relying on the alignment platform being on every classpath).
val jakartaActivationApiVersion = catalog.findVersion("jakarta-activation").get().requiredVersion
val angusActivationVersion = catalog.findVersion("angus-activation").get().requiredVersion

// Internal version-alignment platform bucket.
// The enforced platform (:pulsar-dependencies) must align the build's OWN resolution but must NOT
// leak into the published apiElements/runtimeElements variants: an enforcedPlatform recorded in
// published Gradle Module Metadata forces Pulsar's internal versions onto downstream Gradle
// consumers and strips their ability to override (Gradle's platforms docs warn against using
// enforcedPlatform on a component intended for consumption). Declaring it on a non-consumable,
// non-resolvable bucket that only the resolvable build classpaths extend keeps full build-time
// alignment (and feeds versionMapping for POM versions) while leaving it out of publication.
// Consumers get the separate, non-enforced pulsar-bom for version recommendations instead.
val internalPlatform = configurations.create("internalPlatform") {
    isCanBeConsumed = false
    isCanBeResolved = false
    description = "Enforced version-alignment platform for build resolution only; never published."
}
// Wire the alignment platform onto the build's resolvable classpaths by name. These configurations
// are created by the Java plugin before this convention's body runs, so the name match reliably
// fires for them. (Matching on the mutable isCanBeResolved/isCanBeConsumed flags is NOT reliable for
// configurations created later via `configurations.creating {}` — their flags are set inside the
// creation block, after a flag-based configureEach predicate has already evaluated the legacy
// defaults.) The consumable variants apiElements/runtimeElements are intentionally not listed, so
// the enforced platform stays out of published Gradle Module Metadata. Custom resolvable
// configurations that collect artifacts and need the same alignment (notably the distributions'
// `distLib`, which feeds the binary distribution checked by checkBinaryLicense) extend
// `internalPlatform` explicitly where they are declared.
val platformAlignedClasspaths = setOf(
    "compileClasspath", "runtimeClasspath",
    "testCompileClasspath", "testRuntimeClasspath",
    "annotationProcessor", "testAnnotationProcessor",
)
configurations.matching { it.name in platformAlignedClasspaths }.configureEach {
    extendsFrom(internalPlatform)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(17)
    options.compilerArgs.addAll(listOf("-parameters", "-Xlint:deprecation", "-Xlint:unchecked"))
}

configurations.all {
    // Force Jackson version to match the version catalog. Transitive dependencies
    // (e.g. from jackson-bom) can pull in newer versions that break API compatibility
    // (EnumResolver.constructUsingToString signature changed in 2.19+).
    resolutionStrategy.eachDependency {
        if (requested.group.startsWith("com.fasterxml.jackson")) {
            if (requested.name == "jackson-annotations") {
                useVersion(catalog.findVersion("jackson-annotations").get().requiredVersion)
            } else {
                useVersion(catalog.findVersion("jackson").get().requiredVersion)
            }
        }
    }
}

dependencies {
    // Strip conflicting transitive dependencies from bookkeeper-server at the source. Handling them
    // here (rather than with a build-wide `configurations.all { exclude(...) }`) keeps them off the
    // classpath without leaking a per-dependency <exclusion> onto every dependency in every
    // published POM.
    //   - All BouncyCastle: BookKeeper's transitive bc-fips bundles a CryptoServicesRegistrar that
    //     conflicts with the non-FIPS bcprov-jdk18on. Pulsar manages its own BC deps; the FIPS
    //     modules (bcfips, pulsar-common/pulsar-broker-common tests) declare bc-fips directly.
    //   - log4j-slf4j-impl: the SLF4J 1.x bridge conflicts with Pulsar's SLF4J 2.x setup
    //     (log4j-slf4j2-impl), causing NoSuchMethodError in Log4jLoggerFactory at startup.
    components {
        withModule("org.apache.bookkeeper:bookkeeper-server") {
            allVariants {
                withDependencies {
                    removeAll {
                        it.group == "org.bouncycastle" ||
                            (it.group == "org.apache.logging.log4j" && it.name == "log4j-slf4j-impl")
                    }
                }
            }
        }
        // com.sun.activation:jakarta.activation bundles the jakarta.activation API classes together
        // with the implementation and was not republished past 2.0.x, so it conflicts with
        // jakarta.activation:jakarta.activation-api 2.1.x. Replace it everywhere with the API artifact
        // plus the Eclipse Angus implementation (the EE10 successor). async-http-client still depends
        // on it (https://github.com/AsyncHttpClient/async-http-client/issues/2190) and this rule also
        // guards against any future dependency pulling it in. The replacements carry explicit catalog
        // versions so they resolve on any classpath without relying on the alignment platform being
        // present (e.g. the protobuf plugin's *ProtoPath configurations do not extend it).
        all {
            allVariants {
                withDependencies {
                    if (removeAll { it.group == "com.sun.activation" && it.name == "jakarta.activation" }) {
                        add("jakarta.activation:jakarta.activation-api:$jakartaActivationApiVersion")
                        add("org.eclipse.angus:angus-activation:$angusActivationVersion")
                    }
                }
            }
        }
        // async-http-client depends on the classic io.netty:netty-codec module, which in Netty 4.2
        // is an empty backwards-compatibility aggregator that only adds the unused
        // netty-codec-marshalling and netty-codec-protobuf modules to the classpath. The codec
        // modules async-http-client actually needs (netty-codec-base, netty-codec-compression)
        // come in through its netty-codec-http dependency.
        withModule("org.asynchttpclient:async-http-client") {
            allVariants {
                withDependencies {
                    removeAll { it.group == "io.netty" && it.name == "netty-codec" }
                }
            }
        }
        // libthrift is a transitive dependency of distributedlog-core.
        // libthrift 0.23.0 upgraded to jakarta.* and HttpComponents 5 deps for its HTTP/servlet
        // transports, which distributedlog-core does not use (only TJSON/TMemory serialization is needed).
        // Add a component metadata rule to exclude the unnecessary dependencies.
        withModule("org.apache.thrift:libthrift") {
            allVariants {
                withDependencies {
                    removeAll {
                        (it.group == "jakarta.annotation" && it.name == "jakarta.annotation-api") ||
                        (it.group == "jakarta.servlet" && it.name == "jakarta.servlet-api") ||
                        it.group == "org.apache.httpcomponents.client5" ||
                        it.group == "org.apache.httpcomponents.core5"
                    }
                }
            }
        }
    }

    // Enforced platform pins all dependency versions from the version catalog (the Gradle
    // equivalent of Maven's dependencyManagement). Declared on the internal, non-published
    // `internalPlatform` bucket (see above) so it aligns the build's resolvable classpaths
    // without leaking the enforced platform into published apiElements/runtimeElements metadata.
    "internalPlatform"(enforcedPlatform(project(":pulsar-dependencies")))

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

// Allow overriding the JDK used for running tests via -PtestJavaVersion=17
val testJavaVersion = providers.gradleProperty("testJavaVersion").map { it.toInt() }
val javaToolchains = extensions.getByType<JavaToolchainService>()

tasks.withType<Test>().configureEach {
    testJavaVersion.orNull?.let { version ->
        javaLauncher.set(javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(version))
        })
    }
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
    val ideaActive = providers.systemProperty("idea.active").map { it.toBoolean() }.getOrElse(false)
    val defaultTestRetryCount = if (ideaActive) "0" else "1"
    systemProperty("testRetryCount", providers.gradleProperty("testRetryCount").getOrElse(defaultTestRetryCount))
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
        // Force IPv4 to match Pulsar's runtime scripts (bin/pulsar, bin/bookkeeper). BookKeeper's
        // BookieId validation rejects IPv6 zone identifiers (e.g. fe80::1%lo0), so on hosts where the
        // loopback interface resolves to an IPv6 link-local address (notably macOS) bookies bound to
        // loopback would otherwise fail to start.
        "-Djava.net.preferIPv4Stack=true",
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