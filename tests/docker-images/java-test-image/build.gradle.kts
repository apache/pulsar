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
val dockerTag = providers.gradleProperty("docker.tag").getOrElse("latest")
val dockerPlatforms = providers.gradleProperty("docker.platforms").getOrElse("")

// Dependencies: base pulsar image, java-test-functions jar, buildtools jar
val pulsarDockerBuild = project(":docker:pulsar-docker-image").tasks.named("dockerBuild")
val testFunctionsJar = project(":tests:java-test-functions").tasks.named("shadowJar")
val buildtoolsJar = project(":buildtools").tasks.named("jar")

// Prepare the build context in build/target/
val prepareBuildContext by tasks.registering(Sync::class) {
    dependsOn(testFunctionsJar, buildtoolsJar)

    // Copy scripts from docker/pulsar/scripts and latest-version-image/scripts
    from("${rootDir}/docker/pulsar/scripts") {
        into("scripts")
    }
    from("${projectDir}/../latest-version-image/scripts") {
        into("scripts")
    }

    // Copy certificate-authority
    from("${rootDir}/tests/certificate-authority") {
        into("certificate-authority")
    }

    // Copy supervisor conf files
    from("${projectDir}/../latest-version-image/conf") {
        into("conf")
    }

    // Copy java-test-functions.jar
    from(testFunctionsJar.map { (it as Jar).archiveFile }) {
        rename { "java-test-functions.jar" }
    }

    // Copy buildtools.jar
    from(buildtoolsJar.map { (it as Jar).archiveFile }) {
        rename { "buildtools.jar" }
    }

    into("${projectDir}/target")
}

val dockerBuild by tasks.registering(Exec::class) {
    group = "docker"
    description = "Build the java-test-image Docker image"

    dependsOn(pulsarDockerBuild, prepareBuildContext)

    val imageName = "${dockerOrganization}/java-test-image:${dockerTag}"
    val pulsarImage = "${dockerOrganization}/pulsar:${dockerTag}"

    workingDir = projectDir

    val args = mutableListOf(
        "docker", "build",
        "-t", imageName,
        "--build-arg", "PULSAR_IMAGE=${pulsarImage}",
    )

    if (dockerPlatforms.isNotEmpty()) {
        args.addAll(listOf("--platform", dockerPlatforms))
    }

    args.add(".")

    commandLine(args)
}
