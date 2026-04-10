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
    id("pulsar.java-conventions")
}

// Distribution module — no Java compilation needed
tasks.named("compileJava") { enabled = false }
tasks.named("compileTestJava") { enabled = false }
tasks.named("jar") { enabled = false }

val bookkeeperVersion: String = libs.versions.bookkeeper.get()
val zookeeperVersion: String = libs.versions.zookeeper.get()
val kotlinStdlibVersion: String = libs.versions.kotlin.stdlib.get()
val nettyTcnativeVersion: String = libs.versions.netty.tcnative.get()
val audienceAnnotationsVersion: String = libs.versions.audience.annotations.get()
val jetbrainsAnnotationsVersion: String = libs.versions.jetbrains.annotations.get()

// Configuration for collecting runtime dependencies
val distLib by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    isTransitive = true
    // Inherit version constraints from the root project's version catalog
    extendsFrom(configurations["implementation"])
    // Global exclusions
    exclude(group = "org.projectlombok", module = "lombok")
    // Exclude test frameworks that leak through transitive deps
    exclude(group = "junit", module = "junit")
    exclude(group = "org.hamcrest", module = "hamcrest-core")
    // Jars not in the server distribution
    exclude(group = "com.amazonaws")
    exclude(group = "com.fasterxml.jackson.dataformat", module = "jackson-dataformat-cbor")
    exclude(group = "io.vertx", module = "vertx-grpc")
    exclude(group = "javax.annotation", module = "javax.annotation-api")
    exclude(group = "javax.xml.bind", module = "jaxb-api")
    exclude(group = "joda-time", module = "joda-time")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.apache.bookkeeper.tests")
    exclude(group = "org.bouncycastle", module = "bc-fips")
    // kqueue not in server distribution
    exclude(group = "io.netty", module = "netty-transport-native-kqueue")
    // Kotlin compat jars not in Maven dist
    exclude(group = "org.jetbrains.kotlin", module = "kotlin-stdlib-jdk7")
    exclude(group = "org.jetbrains.kotlin", module = "kotlin-stdlib-jdk8")
    // Exclude non-JPMS JNA (we add jpms variants explicitly)
    exclude(group = "net.java.dev.jna", module = "jna")
    exclude(group = "net.java.dev.jna", module = "jna-platform")
    // grpc modules not in server distribution (grpc-all transitively includes these)
    exclude(group = "io.grpc", module = "grpc-netty")
    exclude(group = "io.grpc", module = "grpc-okhttp")
    exclude(group = "io.grpc", module = "grpc-testing")
    // Original zookeeper excluded — replaced by patched version
    exclude(group = "org.apache.zookeeper", module = "zookeeper")
    // Android annotations not in server dist
    exclude(group = "com.google.android", module = "annotations")
    // Annotation libraries not needed at runtime
    exclude(group = "org.codehaus.mojo", module = "animal-sniffer-annotations")
}

// Resolvable configurations for cross-project artifact dependencies.
// Using configurations instead of direct task references (project().tasks.named())
// ensures compatibility with Gradle's configure-on-demand feature.
val runtimeAllShadowJar by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    isTransitive = false
}
val apiExamplesJar by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    isTransitive = false
}

dependencies {
    // Version constraints from the enforced platform (inherited via implementation,
    // which distLib extends) ensure consistent versions without manual resolutionStrategy.
    distLib(project(":pulsar-broker"))
    distLib(project(":pulsar-metadata"))
    distLib(project(":pulsar-docs-tools"))
    distLib(project(":pulsar-proxy"))
    distLib(project(":pulsar-broker-auth-oidc"))
    distLib(project(":pulsar-broker-auth-sasl"))
    distLib(project(":pulsar-client-auth-sasl"))
    distLib(project(":jetty-upgrade:pulsar-bookkeeper-prometheus-metrics-provider"))
    distLib(project(":jetty-upgrade:pulsar-zookeeper-prometheus-metrics"))
    distLib(project(":pulsar-package-management:pulsar-package-bookkeeper-storage")) {
        exclude(group = "org.objenesis")
    }
    distLib(project(":pulsar-package-management:pulsar-package-filesystem-storage"))
    distLib(project(":pulsar-client-tools"))
    distLib(project(":pulsar-testclient"))
    distLib(project(":pulsar-functions:pulsar-functions-worker")) {
        exclude(group = "io.grpc")
        exclude(group = "org.bouncycastle")
    }
    distLib(project(":pulsar-functions:pulsar-functions-local-runner-original")) {
        exclude(group = "io.grpc")
    }

    // Patched zookeeper (replaces the excluded original)
    distLib(project(":jetty-upgrade:zookeeper-with-patched-admin"))

    // Logging
    distLib(libs.log4j.api)
    distLib(libs.log4j.core)
    distLib(libs.log4j.web)
    distLib(libs.log4j.layout.template.json)
    distLib(libs.log4j.slf4j2.impl)
    distLib(libs.simpleclient.log4j2)

    // Metrics
    distLib(libs.dropwizardmetrics.core)
    distLib(libs.dropwizardmetrics.graphite) {
        exclude(group = "com.rabbitmq", module = "amqp-client")
    }
    distLib(libs.dropwizardmetrics.jvm)

    // Other
    distLib(libs.jline2)
    distLib(libs.snappy.java)
    distLib(libs.jackson.dataformat.yaml)
    distLib(libs.bcpkix.jdk18on)
    distLib(libs.perfmark.api)
    distLib(libs.grpc.all)

    // JNA (JPMS variants used in Maven distribution)
    distLib("net.java.dev.jna:jna-jpms:${libs.versions.jna.get()}")
    distLib("net.java.dev.jna:jna-platform-jpms:${libs.versions.jna.get()}")

    // BookKeeper HTTP server
    distLib(libs.bookkeeper.http.vertx.server) {
        exclude(group = "io.netty")
    }
    distLib(libs.vertx.core)
    distLib(libs.vertx.web)

    // Bouncy Castle
    distLib(project(":bouncy-castle:bouncy-castle-bc"))

    // BookKeeper native JARs (these modules publish .nar artifacts by default;
    // we exclude .nar files below and add the .jar variants explicitly)
    distLib("org.apache.bookkeeper:circe-checksum:${bookkeeperVersion}") {
        artifact { type = "jar" }
    }
    distLib("org.apache.bookkeeper:cpu-affinity:${bookkeeperVersion}") {
        artifact { type = "jar" }
    }
    distLib("org.apache.bookkeeper:native-io:${bookkeeperVersion}") {
        artifact { type = "jar" }
    }

    // Kotlin stdlib and JetBrains annotations (Maven includes these transitively)
    distLib("org.jetbrains.kotlin:kotlin-stdlib:${kotlinStdlibVersion}")
    distLib("org.jetbrains.kotlin:kotlin-stdlib-common:${kotlinStdlibVersion}")
    distLib("org.jetbrains:annotations:${jetbrainsAnnotationsVersion}")

    // zookeeper-jute (transitive of zookeeper, but zookeeper itself is excluded)
    distLib("org.apache.zookeeper:zookeeper-jute:${zookeeperVersion}")

    // Hadoop annotations (transitive dep needed in server dist)
    distLib("org.apache.yetus:audience-annotations:${audienceAnnotationsVersion}")

    // netty-tcnative with platform classifiers
    distLib("io.netty:netty-tcnative-boringssl-static:${nettyTcnativeVersion}")
    distLib("io.netty:netty-tcnative-boringssl-static:${nettyTcnativeVersion}:linux-x86_64")
    distLib("io.netty:netty-tcnative-boringssl-static:${nettyTcnativeVersion}:linux-aarch_64")
    distLib("io.netty:netty-tcnative-boringssl-static:${nettyTcnativeVersion}:osx-x86_64")
    distLib("io.netty:netty-tcnative-boringssl-static:${nettyTcnativeVersion}:osx-aarch_64")
    distLib("io.netty:netty-tcnative-boringssl-static:${nettyTcnativeVersion}:windows-x86_64")

    // pulsar-broker-common test jar is included in Maven server dist
    // but requires test-fixtures support in pulsar-broker-common; skipped for now

    // Cross-project artifact dependencies for the server distribution tarball
    runtimeAllShadowJar(project(path = ":pulsar-functions:pulsar-functions-runtime-all", configuration = "shadowJarElements"))
    apiExamplesJar(project(":pulsar-functions:pulsar-functions-api-examples"))
}

val pulsarVersion = project.version.toString()
val rootDir = rootProject.projectDir

val serverDistTar by tasks.registering(Tar::class) {
    archiveBaseName.set("apache-pulsar")
    archiveVersion.set(pulsarVersion)
    archiveClassifier.set("bin")
    archiveExtension.set("tar.gz")
    compression = Compression.GZIP
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    // Use a top-level directory in the tarball
    val baseDir = "apache-pulsar-${pulsarVersion}"

    // README, LICENSE, NOTICE
    from("src/assemble/README.bin.txt") {
        rename("README.bin.txt", "README")
        into(baseDir)
    }
    from("src/assemble/LICENSE.bin.txt") {
        rename("LICENSE.bin.txt", "LICENSE")
        into(baseDir)
    }
    from("src/assemble/NOTICE.bin.txt") {
        rename("NOTICE.bin.txt", "NOTICE")
        into(baseDir)
    }

    // conf/ directory — preserve execute permission on shell scripts
    from(rootDir.resolve("conf")) {
        into("${baseDir}/conf")
        eachFile {
            if (file.canExecute()) permissions { unix("755") }
        }
    }

    // bin/ directory with executable permissions
    from(rootDir.resolve("bin")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }

    // licenses/ directory
    from(file("licenses")) {
        into("${baseDir}/licenses")
    }

    // Runtime dependency JARs into lib/
    // Include groupId in jar names to identify provenance (matches Maven assembly outputFileNameMapping)
    from(distLib) {
        into("${baseDir}/lib")
        // These JARs go into other directories (instances/, examples/), not lib/
        exclude("**/pulsar-functions-runtime-all-*.jar")
        exclude("**/pulsar-functions-api-examples-*.jar")
        // Exclude .nar files (BookKeeper NAR artifacts — we add .jar variants explicitly)
        exclude("**/*.nar")
        // Exclude the non-classified netty-transport-native-epoll jar
        // (only the platform-classified variants like -linux-x86_64 belong in the dist)
        exclude {
            val n = it.file.name
            n.startsWith("netty-transport-native-epoll-") && n.endsWith(".jar") &&
                !n.contains("-linux-") && !n.contains("-osx-") && !n.contains("-windows-")
        }
    }

    // Build file-name -> groupId-prefixed-name map from resolved artifacts
    val renameMap = distLib.incoming.artifacts.resolvedArtifacts.map { artifacts ->
        artifacts.associate { result ->
            val id = result.id.componentIdentifier
            val file = result.file
            val ext = file.extension
            val newName = when (id) {
                is org.gradle.api.artifacts.component.ModuleComponentIdentifier -> {
                    // Include classifier from the file name if present
                    // e.g. netty-transport-native-epoll-x.y.z.Final-linux-x86_64.jar
                    val expectedBase = "${id.module}-${id.version}"
                    val nameWithoutExt = file.nameWithoutExtension
                    val classifier = if (nameWithoutExt.startsWith(expectedBase) && nameWithoutExt.length > expectedBase.length) {
                        nameWithoutExt.substring(expectedBase.length) // includes leading dash
                    } else ""
                    "${id.group}-${id.module}-${id.version}${classifier}.${ext}"
                }
                is org.gradle.api.artifacts.component.ProjectComponentIdentifier -> {
                    var mappedName = file.nameWithoutExtension
                    // For bouncy-castle-bc, add -pkg classifier
                    if (mappedName.startsWith("bouncy-castle-bc-")) {
                        mappedName = mappedName + "-pkg"
                    }
                    "org.apache.pulsar-${mappedName}.${ext}"
                }
                else -> file.name
            }
            file.name to newName
        }
    }
    // Rename JARs to groupId-artifactId-version.jar format
    eachFile {
        if (path.startsWith("${baseDir}/lib/")) {
            val map = renameMap.get()
            map[name]?.let { name = it }
        }
    }

    // Python instances
    from(rootDir.resolve("pulsar-functions/instance/src/main/python")) {
        into("${baseDir}/instances/python-instance")
    }

    // Python examples — preserve execute permission
    from(rootDir.resolve("pulsar-functions/python-examples")) {
        into("${baseDir}/examples/python-examples")
        eachFile {
            if (file.canExecute()) permissions { unix("755") }
        }
    }

    // Java instance JAR (runtime-all fat jar, produced by Shadow plugin)
    from(runtimeAllShadowJar) {
        into("${baseDir}/instances")
        rename(".*", "java-instance.jar")
    }

    // Java examples JAR
    from(apiExamplesJar) {
        into("${baseDir}/examples")
        rename(".*", "api-examples.jar")
    }

    // Example config files
    from(rootDir.resolve("pulsar-functions/java-examples/src/main/resources")) {
        into("${baseDir}/examples")
        include("example-function-config.yaml")
        include("example-window-function-config.yaml")
        include("example-stateful-function-config.yaml")
    }

    // Create empty instances/deps directory
    into("${baseDir}/instances/deps") {
        from(files())
    }
}

// Consumable configuration exposing the server distribution tarball
val serverDistElements by configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    outgoing {
        artifact(serverDistTar)
    }
}

tasks.named("assemble") {
    dependsOn(serverDistTar)
}

// Export the runtime classpath to a file for bin/ scripts to use
// when running Pulsar from a development build (without lib/ directory)
val exportClasspath by tasks.registering {
    val outputFile = layout.buildDirectory.file("classpath.txt")
    outputs.file(outputFile)
    doLast {
        outputFile.get().asFile.apply {
            parentFile.mkdirs()
            writeText(distLib.asPath)
        }
    }
}
