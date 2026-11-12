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
val dockerInstallAsyncProfiler = providers.gradleProperty("docker.install.asyncprofiler").getOrElse("false")

// The jars of other projects that go into the image, see ProjectArtifacts.kt
val testFunctionsJar = projectJars("testFunctionsJar", ":tests:java-test-functions").files
val buildtoolsJar = projectJars("buildtoolsJar", ":buildtools").files

// Prepare the build context in build/target/
val prepareBuildContext = tasks.register<Sync>("prepareBuildContext") {
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

// Whether the test image installs the offloader of a 4.x release image, which only the scenarios that offload need
val installOffloaders = providers.gradleProperty("performance.installOffloaders").getOrElse("false")

/**
 * Registers a `docker build` of the test image on [pulsarImage], and shares the image's ID with other projects as
 * [sharedArtifact] (see SharedBuildArtifacts.kt). With [pulsarImageArtifact], the Pulsar image is the one that
 * :docker:pulsar-docker-image builds from this repository and shares under that name, and the test image is rebuilt
 * only when it or the build context changes. Without it, the Pulsar image is a released one: it is pulled on every
 * build, since a tag such as `latest` moves.
 */
fun registerDockerBuild(taskName: String, sharedArtifact: String, imageTag: String, installAsyncProfiler: String,
                        pulsarImage: String = "${dockerOrganization}/pulsar:${dockerTag}",
                        pulsarImageArtifact: String? = "docker-image:pulsar"): TaskProvider<Exec> {
    val task = tasks.register<Exec>(taskName) {
        group = "docker"

        val imageName = "${dockerOrganization}/java-test-image:${imageTag}"
        val imageIdFile = layout.buildDirectory.file("docker/${taskName}.iid").get().asFile
        val asyncProfilerVersion = libs.versions.async.profiler.get()

        workingDir = projectDir

        val args = mutableListOf(
            "docker", "build",
            "-t", imageName,
            "--iidfile", imageIdFile.absolutePath,
            "--build-arg", "PULSAR_IMAGE=${pulsarImage}",
            "--build-arg", "INSTALL_ASYNC_PROFILER=${installAsyncProfiler}",
            // The offloader of a 4.x release image, which -Pperformance.installOffloaders=true installs for the
            // scenarios that offload
            "--build-arg", "INSTALL_OFFLOADERS=${installOffloaders}",
            "--build-arg", "ASYNC_PROFILER_VERSION=${asyncProfilerVersion}"
        )

        if (dockerPlatforms.isNotEmpty()) {
            args.addAll(listOf("--platform", dockerPlatforms))
        }
        if (pulsarImageArtifact == null) {
            args.add("--pull")
        }

        args.add(".")

        commandLine(args)
        // docker writes the image ID file but doesn't create its directory. Gradle creates it for a declared output,
        // which a build on a released Pulsar image, such as dockerBuildCluster's, doesn't have
        doFirst {
            imageIdFile.parentFile.mkdirs()
        }

        inputs.file("Dockerfile")
        inputs.files(prepareBuildContext)
        inputs.property("dockerBuildArgs", args)
        if (pulsarImageArtifact != null) {
            // The ID of the Pulsar image, which builds it first, so that a new base image rebuilds this one
            inputs.files(sharedBuildArtifact(":docker:pulsar-docker-image", pulsarImageArtifact))
                .withPropertyName("pulsarImageId")
            // Rebuild the image only when what goes into it changes, see dockerImageOutput
            dockerImageOutput(imageName, imageIdFile)
        } else {
            outputs.upToDateWhen { false }
        }
    }
    shareBuildArtifact(sharedArtifact, layout.buildDirectory.file("docker/${taskName}.iid"), task)
    return task
}

val dockerBuild = registerDockerBuild("dockerBuild", "docker-image:java-test-image", dockerTag,
    dockerInstallAsyncProfiler)
dockerBuild.configure {
    description = "Build the java-test-image Docker image"
}

// A separate image so that a profiling run never replaces the image the other integration tests use,
// and so that the async-profiler download stays out of the ordinary (and CI) test image build.
// :tests:integration:profilingIntegrationTest builds and uses this one.
val dockerBuildWithAsyncProfiler =
    registerDockerBuild("dockerBuildWithAsyncProfiler", "docker-image:java-test-image-asyncprofiler",
        "${dockerTag}-asyncprofiler", "true")
dockerBuildWithAsyncProfiler.configure {
    description = "Build the java-test-image Docker image with async-profiler installed"
}

// The glibc-based variant on top of the Wolfi Pulsar image, for the jonoffcpu profiler agent whose native
// libraries do not load on musl. :tests:performance:launcher:profile builds and uses this one.
val dockerBuildWolfi = registerDockerBuild("dockerBuildWolfi", "docker-image:java-test-image-wolfi",
    "${dockerTag}-wolfi", "false", "${dockerOrganization}/pulsar:${dockerTag}-wolfi", "docker-image:pulsar-wolfi")
dockerBuildWolfi.configure {
    description = "Build the java-test-image Docker image from the Wolfi Pulsar image under the <tag>-wolfi tag"
}

// The cluster's image for a performance run of a released Pulsar, -Pperformance.clusterPulsarImage=<image> such as
// apachepulsar/pulsar:4.0.13: the test image built on that Pulsar image, whose Alpine base the Dockerfile needs.
// :tests:performance:launcher:run and :profile build and use it for ZooKeeper, the bookies and the brokers; the
// workloads, and so the Pulsar client, stay on this repository's test image.
val clusterPulsarImage = providers.gradleProperty("performance.clusterPulsarImage").orNull
if (clusterPulsarImage != null) {
    val dockerBuildCluster = registerDockerBuild("dockerBuildCluster", "docker-image:java-test-image-cluster",
        "cluster-" + clusterPulsarImage.replace(Regex("[^A-Za-z0-9_.-]"), "-").takeLast(120), "false",
        clusterPulsarImage, null)
    dockerBuildCluster.configure {
        description = "Build the java-test-image Docker image on the performance.clusterPulsarImage Pulsar image"
    }
}

// The clusters' images for :tests:performance:launcher:compareImages and compareImageWithCheckout, which compare two
// released Pulsar images, -Pperformance.compare.baselineImage and -Pperformance.compare.candidateImage, or a released
// image and this checkout: the test image built on each, named as dockerBuildCluster names its image.
listOf("Baseline" to "performance.compare.baselineImage", "Candidate" to "performance.compare.candidateImage")
    .forEach { (side, property) ->
        providers.gradleProperty(property).orNull?.let { image ->
            registerDockerBuild("dockerBuildCompare$side",
                "docker-image:java-test-image-compare-${side.lowercase()}", "cluster-" + image.replace(Regex("[^A-Za-z0-9_.-]"), "-").takeLast(120), "false", image, null)
                .configure {
                    description = "Build the java-test-image Docker image on the $property Pulsar image"
                }
        }
    }
