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

import java.io.File
import org.gradle.api.tasks.Exec

/**
 * Makes a `docker build` task incremental, so that tasks that need the image can depend on it without
 * rebuilding it every time. The task has to pass `--iidfile` with [imageIdFile] to `docker build`, and to
 * declare the files and build arguments that go into the image as its inputs. It is up to date when its
 * inputs haven't changed and [imageName] still refers to the image it built: removing the image, or building
 * another image under the same name, builds it again.
 */
fun Exec.dockerImageOutput(imageName: String, imageIdFile: File) {
    outputs.file(imageIdFile)
    outputs.upToDateWhen {
        imageIdFile.isFile && imageIdFile.readText().trim() == dockerImageId(imageName)
    }
}

/** The ID of the local image [imageName], or null when there is none or Docker isn't available. */
private fun dockerImageId(imageName: String): String? = runCatching {
    val process = ProcessBuilder("docker", "image", "inspect", "--format", "{{.Id}}", imageName)
        .redirectError(ProcessBuilder.Redirect.DISCARD)
        .start()
    val output = process.inputStream.bufferedReader().use { it.readText().trim() }
    output.takeIf { process.waitFor() == 0 }
}.getOrNull()
