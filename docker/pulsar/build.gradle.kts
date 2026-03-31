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

group = "org.apache.pulsar"
version = the<VersionCatalogsExtension>().named("libs").findVersion("pulsar").get().requiredVersion


val pulsarVersion = project.version.toString()
val dockerOrganization = providers.gradleProperty("docker.organization").getOrElse("apachepulsar")
val dockerImage = providers.gradleProperty("docker.image").getOrElse("pulsar")
val dockerTag = providers.gradleProperty("docker.tag").getOrElse("latest")
val dockerPlatforms = providers.gradleProperty("docker.platforms").getOrElse("")
val useWolfi = providers.gradleProperty("docker.wolfi").isPresent

val serverDistTask = project(":distribution:pulsar-server-distribution").tasks.named("serverDistTar")
val offloaderDistTask = project(":distribution:pulsar-offloader-distribution").tasks.named("offloaderDistTar")

// Copy the server tarball into target/ (Docker build context)
val copyTarball by tasks.registering(Copy::class) {
    dependsOn(serverDistTask)
    from(serverDistTask.map { (it as Tar).archiveFile })
    into(layout.buildDirectory.dir("target"))
}

// Copy offloader tarball into build context
val copyOffloaderTarball by tasks.registering(Copy::class) {
    dependsOn(offloaderDistTask)
    from(offloaderDistTask.map { (it as Tar).archiveFile })
    into(layout.buildDirectory.dir("target"))
}

val dockerBuild by tasks.registering(Exec::class) {
    group = "docker"
    description = "Build the Pulsar Docker image"

    dependsOn(copyTarball, copyOffloaderTarball)

    val dockerfile = if (useWolfi) "Dockerfile.wolfi" else "Dockerfile"
    val imageName = "${dockerOrganization}/${dockerImage}:${dockerTag}"
    val tarballName = "apache-pulsar-${pulsarVersion}-bin.tar.gz"
    val offloaderTarballName = "apache-pulsar-offloaders-${pulsarVersion}-bin.tar.gz"
    // Resolve version catalog values at configuration time (not in doFirst)
    val pythonClientVersion = libs.versions.pulsar.client.python.get()
    val snappyVersion = libs.versions.snappy.get()
    val jdkMajorVersion = libs.versions.docker.jdk.get()

    // Docker build context is the project directory
    workingDir = projectDir

    val args = mutableListOf(
        "docker", "build",
        "-f", dockerfile,
        "-t", imageName,
        "--build-arg", "PULSAR_TARBALL=build/target/${tarballName}",
        "--build-arg", "PULSAR_CLIENT_PYTHON_VERSION=${pythonClientVersion}",
        "--build-arg", "SNAPPY_VERSION=${snappyVersion}",
        "--build-arg", "IMAGE_JDK_MAJOR_VERSION=${jdkMajorVersion}",
        "--build-arg", "PULSAR_OFFLOADER_TARBALL=build/target/${offloaderTarballName}",
    )

    if (dockerPlatforms.isNotEmpty()) {
        args.addAll(listOf("--platform", dockerPlatforms))
    }

    args.add(".")

    commandLine(args)
}
