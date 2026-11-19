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

import org.gradle.api.GradleException
import org.gradle.api.artifacts.component.ProjectComponentIdentifier
import org.gradle.api.artifacts.result.ResolvedArtifactResult
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.process.CommandLineArgumentProvider

/**
 * Passes the paths of the archives that tests load, such as NARs, to the test JVM as system properties.
 * The archives are resolved when the test runs; the task declares them as its inputs separately (see
 * ProjectArtifacts.kt), so only the property names are inputs here.
 *
 * @param nars the resolved NARs, one per project
 * @param jars the resolved JARs, of which the first is passed
 * @param narProperties the system property for the NAR of each project path
 * @param jarProperties the system properties for the first JAR
 */
class TestArchivePaths(
    @get:Internal val nars: Provider<Set<ResolvedArtifactResult>>,
    @get:Internal val jars: Provider<Set<ResolvedArtifactResult>>,
    @get:Input val narProperties: Map<String, String>,
    @get:Input val jarProperties: List<String>,
) : CommandLineArgumentProvider {
    override fun asArguments(): Iterable<String> {
        val narByProject = nars.get().associate {
            (it.id.componentIdentifier as ProjectComponentIdentifier).projectPath to it.file.absolutePath
        }
        val narArguments = narProperties.map { (property, projectPath) ->
            val path = narByProject[projectPath] ?: throw GradleException("No NAR of $projectPath was resolved")
            "-D$property=$path"
        }
        val jarPath = jars.get().first().file.absolutePath
        return narArguments + jarProperties.map { "-D$it=$jarPath" }
    }
}
