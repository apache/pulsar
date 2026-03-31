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
val golangImage = providers.gradleProperty("docker.golang.image").getOrElse("golang:1.24-alpine")

// Dependencies: pulsar image, java-test-functions jar, java-test-plugins jar, buildtools jar
val pulsarDockerBuild = project(":docker:pulsar-docker-image").tasks.named("dockerBuild")
val testFunctionsJar = project(":tests:java-test-functions").tasks.named("shadowJar")
val testPluginsJar = project(":tests:java-test-plugins").tasks.named("jar")
val buildtoolsJar = project(":buildtools").tasks.named("jar")

// Prepare the build context in target/ (to match Dockerfile COPY paths)
val prepareBuildContext by tasks.registering(Sync::class) {
    dependsOn(testFunctionsJar, testPluginsJar, buildtoolsJar)

    // Copy pulsar-function-go source
    from("${rootDir}/pulsar-function-go") {
        into("pulsar-function-go")
    }

    // Copy certificate-authority
    from("${rootDir}/tests/certificate-authority") {
        into("certificate-authority")
    }

    // Copy java-test-functions.jar
    from(testFunctionsJar.map { (it as Jar).archiveFile }) {
        rename { "java-test-functions.jar" }
    }

    // Copy java-test-plugins as .nar
    from(testPluginsJar.map { (it as Jar).archiveFile }) {
        rename { "java-test-plugins.nar" }
        into("plugins")
    }

    // Copy buildtools.jar
    from(buildtoolsJar.map { (it as Jar).archiveFile }) {
        rename { "buildtools.jar" }
    }

    into("${projectDir}/target")
}

val dockerBuild by tasks.registering(Exec::class) {
    group = "docker"
    description = "Build the pulsar-test-latest-version Docker image"

    dependsOn(pulsarDockerBuild, prepareBuildContext)

    val imageName = "${dockerOrganization}/pulsar-test-latest-version:${dockerTag}"
    val pulsarImage = "${dockerOrganization}/pulsar:${dockerTag}"

    workingDir = projectDir

    val args = mutableListOf(
        "docker", "build",
        "-t", imageName,
        "--build-arg", "PULSAR_IMAGE=${pulsarImage}",
        "--build-arg", "GOLANG_IMAGE=${golangImage}",
    )

    if (dockerPlatforms.isNotEmpty()) {
        args.addAll(listOf("--platform", dockerPlatforms))
    }

    args.add(".")

    commandLine(args)
}
