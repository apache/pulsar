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
}

group = "org.apache.pulsar"
version = rootProject.version

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

// Substitute org.lz4:lz4-java with at.yawk.lz4:lz4-java (Pulsar uses the fork)
configurations.all {
    resolutionStrategy.dependencySubstitution {
        substitute(module("org.lz4:lz4-java")).using(module("at.yawk.lz4:lz4-java:1.10.3"))
    }
}

// Access the version catalog
val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    // BOMs / platforms
    api(platform(libs.netty.bom))
    api(platform(libs.jackson.bom))
    api(platform(libs.grpc.bom))
    api(platform(libs.slf4j.bom))
    api(platform(libs.log4j.bom))
    api(platform(libs.mockito.bom))
    api(platform(libs.prometheus.bom))
    api(platform(libs.jjwt.bom))

    // Lombok (annotation processor for all modules)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    // Common test dependencies
    testImplementation(libs.testng)
    testImplementation(libs.mockito.core)
    testImplementation(libs.assertj.core)
    // Netty needed on test classpath for ExtendedNettyLeakDetector
    testRuntimeOnly(libs.netty.common)
    testRuntimeOnly(libs.netty.buffer)
    // Jetty SLF4J logging bridge needed for tests that use embedded Jetty
    testRuntimeOnly("org.eclipse.jetty:jetty-slf4j-impl:${libs.versions.jetty.get()}")
    // OpenTelemetry incubator API needed by client tests
    testRuntimeOnly(libs.opentelemetry.api.incubator)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf(
        "-parameters",
        "-Xlint:-options",
    ))
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:none", "-quiet")
        encoding = "UTF-8"
    }
}

// Add certificate-authority test resources from the tests/ directory
sourceSets.test {
    resources.srcDir(rootProject.file("tests"))
}

tasks.withType<Test>().configureEach {
    useTestNG {
        excludeGroups("quarantine", "flaky")
        // Only add listeners if buildtools is on the test classpath
        val hasBuildtools = classpath.any { it.path.contains("buildtools") }
        if (hasBuildtools) {
            listeners.addAll(listOf(
                "org.apache.pulsar.tests.PulsarTestListener",
                "org.apache.pulsar.tests.AnnotationListener",
                "org.apache.pulsar.tests.FailFastNotifier",
                "org.apache.pulsar.tests.MockitoCleanupListener",
                "org.apache.pulsar.tests.FastThreadLocalCleanupListener",
            ))
        }
    }

    maxParallelForks = (project.findProperty("testForkCount") as? String)?.toInt() ?: 4
    maxHeapSize = project.findProperty("testMaxHeapSize") as? String ?: "1300m"

    jvmArgs(
        // Module access required for tests on JDK 17+
        "--add-opens", "java.base/jdk.internal.loader=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.io=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
        "--add-opens", "java.base/sun.net=ALL-UNNAMED",
        "--add-opens", "java.management/sun.management=ALL-UNNAMED",
        "--add-opens", "jdk.management/com.sun.management.internal=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.platform=ALL-UNNAMED",
        // Byte-buddy / Mockito agent
        "-XX:+EnableDynamicAgentLoading",
        "-Xshare:off",
        // Netty optimized direct memory access
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dorg.apache.pulsar.shade.io.netty.tryReflectionSetAccessible=true",
        "-Dpulsar.allocator.pooled=true",
        "-Dpulsar.allocator.exit_on_oom=false",
        "-Dpulsar.allocator.out_of_memory_policy=FallbackToHeap",
        // GC for leak detection
        "-XX:+UnlockExperimentalVMOptions",
        // Netty leak detection
        "-Dio.netty.customResourceLeakDetector=org.apache.pulsar.tests.ExtendedNettyLeakDetector",
        "-Dio.netty.leakDetection.level=paranoid",
        "-Dio.netty.leakDetection.targetRecords=16",
        "-Dio.netty.leakDetection.acquireAndReleaseOnly=true",
        "-Dio.netty.leakDetection.samplingInterval=32",
        // Heap dumps on OOM
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:HeapDumpPath=/tmp",
    )

    // JDK 24+ specific args
    if (JavaVersion.current() >= JavaVersion.VERSION_24) {
        jvmArgs(
            "--enable-native-access=ALL-UNNAMED",
            "--sun-misc-unsafe-memory-access=allow",
        )
    }

    systemProperty("testRetryCount", project.findProperty("testRetryCount") ?: "1")
    systemProperty("testFailFast", project.findProperty("testFailFast") ?: "true")

    // Redirect test output to files
    testLogging {
        showStandardStreams = false
    }

    reports {
        junitXml.required.set(true)
        html.required.set(true)
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
            "Built-By" to "Apache Pulsar",
        )
    }
}
