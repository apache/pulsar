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
val nettyTcnativeVersion: String = libs.versions.netty.tcnative.get()
val bookkeeperVersion: String = libs.versions.bookkeeper.get()
val distLib by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    isTransitive = true
    // Inherit version constraints from the enforced platform (via implementation)
    extendsFrom(configurations["implementation"])
    exclude(group = "org.projectlombok", module = "lombok")
    // Exclude jars not in the shell distribution
    exclude(group = "javax.xml.bind", module = "jaxb-api")
    exclude(group = "net.java.dev.jna", module = "jna")
    exclude(group = "net.java.dev.jna", module = "jna-platform")
    exclude(group = "io.netty", module = "netty-transport-native-kqueue")
    exclude(group = "io.prometheus", module = "simpleclient_caffeine")
}
dependencies {
    distLib(project(":pulsar-client-tools"))
    distLib(libs.log4j.core)
    distLib(libs.log4j.web)
    distLib(libs.log4j.layout.template.json)
    distLib(libs.log4j.slf4j2.impl)
    distLib(libs.simpleclient.log4j2)
    // Bouncy Castle
    distLib(project(":bouncy-castle:bouncy-castle-bc"))
    // conscrypt (in Maven shell dist)
    distLib(libs.conscrypt.openjdk.uber)
    // swagger-annotations (in Maven shell dist)
    distLib(libs.swagger.annotations)
    // netty-tcnative with platform classifiers (including windows)
    distLib("io.netty:netty-tcnative-boringssl-static:${nettyTcnativeVersion}:windows-x86_64")
    // BookKeeper native JARs (these modules publish .nar by default;
    // we exclude .nar files below and add the .jar variants explicitly)
    distLib("org.apache.bookkeeper:circe-checksum:${bookkeeperVersion}") {
        artifact { type = "jar" }
    }
    distLib("org.apache.bookkeeper:cpu-affinity:${bookkeeperVersion}") {
        artifact { type = "jar" }
    }
}
val pulsarVersion = project.version.toString()
val rootDir = rootProject.projectDir
val shellDistTar by tasks.registering(Tar::class) {
    val baseDir = "apache-pulsar-shell-${pulsarVersion}"
    val renameMap = distLib.incoming.artifacts.resolvedArtifacts.map { artifacts ->
        artifacts.associate { result ->
            val id = result.id.componentIdentifier
            val file = result.file
            val newName = when (id) {
                is org.gradle.api.artifacts.component.ProjectComponentIdentifier -> {
                    var mappedName = file.nameWithoutExtension
                    if (mappedName.startsWith("bouncy-castle-bc-")) {
                        mappedName = mappedName + "-pkg"
                    }
                    "${mappedName}.${file.extension}"
                }
                else -> file.name
            }
            file.name to newName
        }
    }
    archiveBaseName.set("apache-pulsar-shell")
    archiveVersion.set(pulsarVersion)
    archiveClassifier.set("bin")
    archiveExtension.set("tar.gz")
    compression = Compression.GZIP
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from("src/assemble/LICENSE.bin.txt") {
        rename("LICENSE.bin.txt", "LICENSE")
        into(baseDir)
    }
    from("src/assemble/NOTICE.bin.txt") {
        rename("NOTICE.bin.txt", "NOTICE")
        into(baseDir)
    }
    from("src/assemble/README") {
        into(baseDir)
    }
    // Shell scripts
    from(rootDir.resolve("bin/pulsar-admin-common.sh")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }
    from(rootDir.resolve("bin/pulsar-shell")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }
    from(rootDir.resolve("bin/pulsar-admin-common.cmd")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }
    from(rootDir.resolve("bin/pulsar-shell.cmd")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }
    // Config files
    from(rootDir.resolve("conf/client.conf")) {
        into("${baseDir}/conf")
    }
    from(rootDir.resolve("conf/log4j2.yaml")) {
        into("${baseDir}/conf")
    }
    // Runtime dependency JARs
    from(distLib) {
        into("${baseDir}/lib")
        exclude("**/*.nar")
    }
    // Rename project JARs to match Maven artifact names
    eachFile {
        if (path.startsWith("${baseDir}/lib/")) {
            val map = renameMap.get()
            map[name]?.let { name = it }
        }
    }
}
val shellDistZip by tasks.registering(Zip::class) {
    val baseDir = "apache-pulsar-shell-${pulsarVersion}"
    val renameMap = distLib.incoming.artifacts.resolvedArtifacts.map { artifacts ->
        artifacts.associate { result ->
            val id = result.id.componentIdentifier
            val file = result.file
            val newName = when (id) {
                is org.gradle.api.artifacts.component.ProjectComponentIdentifier -> {
                    var mappedName = file.nameWithoutExtension
                    if (mappedName.startsWith("bouncy-castle-bc-")) {
                        mappedName = mappedName + "-pkg"
                    }
                    "${mappedName}.${file.extension}"
                }
                else -> file.name
            }
            file.name to newName
        }
    }
    archiveBaseName.set("apache-pulsar-shell")
    archiveVersion.set(pulsarVersion)
    archiveClassifier.set("bin")
    archiveExtension.set("zip")
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from("src/assemble/LICENSE.bin.txt") {
        rename("LICENSE.bin.txt", "LICENSE")
        into(baseDir)
    }
    from("src/assemble/NOTICE.bin.txt") {
        rename("NOTICE.bin.txt", "NOTICE")
        into(baseDir)
    }
    from("src/assemble/README") {
        into(baseDir)
    }
    from(rootDir.resolve("bin/pulsar-admin-common.sh")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }
    from(rootDir.resolve("bin/pulsar-shell")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }
    from(rootDir.resolve("bin/pulsar-admin-common.cmd")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }
    from(rootDir.resolve("bin/pulsar-shell.cmd")) {
        into("${baseDir}/bin")
        filePermissions { unix("755") }
    }
    from(rootDir.resolve("conf/client.conf")) {
        into("${baseDir}/conf")
    }
    from(rootDir.resolve("conf/log4j2.yaml")) {
        into("${baseDir}/conf")
    }
    from(distLib) {
        into("${baseDir}/lib")
        exclude("**/*.nar")
    }
    eachFile {
        if (path.startsWith("${baseDir}/lib/")) {
            val map = renameMap.get()
            map[name]?.let { name = it }
        }
    }
}
tasks.named("assemble") {
    dependsOn(shellDistTar, shellDistZip)
}

// Export the runtime classpath to a file for bin/ scripts to use
// when running Pulsar CLI tools from a development build
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
