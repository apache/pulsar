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

val pulsarVersion = project.version.toString()
val dockerOrganization = providers.gradleProperty("docker.organization").getOrElse("apachepulsar")
val dockerTag = providers.gradleProperty("docker.tag").getOrElse("latest")
val dockerPlatforms = providers.gradleProperty("docker.platforms").getOrElse("")

// Ensure the parent project is configured before resolving cross-project task references.
// Required for --configure-on-demand: the Kotlin DSL needs parent ClassLoaderScopes to be locked.
evaluationDependsOn(":docker")

// Resolvable configurations for cross-project artifact dependencies.
// Using configurations instead of direct task references (project().tasks.named())
// ensures compatibility with Gradle's configure-on-demand feature.
val testFunctionsJar by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    isTransitive = false
}
val buildtoolsJar by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    isTransitive = false
}

dependencies {
    testFunctionsJar(project(":tests:java-test-functions"))
    buildtoolsJar(project(":buildtools"))
}

// Prepare the build context in build/target/
val prepareBuildContext by tasks.registering(Sync::class) {
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
    from(testFunctionsJar) {
        rename { "java-test-functions.jar" }
    }

    // Copy buildtools.jar
    from(buildtoolsJar) {
        rename { "buildtools.jar" }
    }

    into("${projectDir}/target")
}

val dockerBuild by tasks.registering(Exec::class) {
    group = "docker"
    description = "Build the java-test-image Docker image"

    dependsOn(":docker:pulsar-docker-image:dockerBuild", prepareBuildContext)

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
