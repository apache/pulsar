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

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecOperations
import java.io.ByteArrayOutputStream
import java.io.File
import javax.inject.Inject

/**
 * Converts JFR recordings into flame graphs with async-profiler's `jfrconv`.
 *
 * Each recording gets a directory next to it named after the file without its extension plus a
 * `-flamegraphs` suffix — `profile.jfr` produces `profile-flamegraphs/` — holding one flame graph
 * per view. A view is a profiling event (`cpu`, `wall`, `alloc`, `lock`, …) rendered three ways:
 * merged across threads (`cpu.html`), split per thread (`cpu_threads.html`) and grouped into
 * async-profiler's categories (`cpu_classify.html`).
 *
 * A recording rarely contains every event: a run without allocation profiling has nothing to put in
 * an `alloc` view. Such a view is skipped with a message rather than failing the task, so that one
 * absent event does not cost the views that could be produced.
 */
abstract class JfrFlamegraphsTask : DefaultTask() {

    /** The recordings to convert. */
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NAME_ONLY)
    abstract val jfrFiles: ConfigurableFileCollection

    /** async-profiler's `jfrconv`. */
    @get:Input
    abstract val jfrconvExecutable: Property<String>

    /** Profiling events to render, one directory entry per event and rendering. */
    @get:Input
    abstract val profileTypes: ListProperty<String>

    @get:Inject
    abstract val execOperations: ExecOperations

    @TaskAction
    fun convert() {
        val recordings = jfrFiles.files.filter { it.isFile }.sortedBy { it.absolutePath }
        if (recordings.isEmpty()) {
            throw GradleException("No JFR recordings to convert. Point -Pjfr at a .jfr file or at a "
                + "directory containing one.")
        }
        val jfrconv = jfrconvExecutable.get()
        val types = profileTypes.get()
        var produced = 0
        var skipped = 0
        for (recording in recordings) {
            val baseName = recording.name.substringBeforeLast('.')
            val outputDir = File(recording.parentFile, "$baseName-flamegraphs")
            if (!outputDir.isDirectory && !outputDir.mkdirs()) {
                throw GradleException("Cannot create flame graph directory ${outputDir.absolutePath}")
            }
            for (type in types) {
                // Merged, per-thread and category-grouped renderings of the same event.
                for ((suffix, extraArgs) in listOf(
                    "" to emptyList(),
                    "_threads" to listOf("--threads"),
                    "_classify" to listOf("--classify"),
                )) {
                    val output = File(outputDir, "$type$suffix.html")
                    val title = "$baseName $type" + if (suffix.isEmpty()) "" else " (${suffix.trimStart('_')})"
                    if (runJfrconv(jfrconv, recording, output, type, extraArgs, title)) {
                        produced++
                    } else {
                        skipped++
                    }
                }
            }
            logger.lifecycle("Flame graphs for {}: {}", recording.name, outputDir.absolutePath)
        }
        logger.lifecycle("Wrote {} flame graph(s) from {} recording(s){}", produced, recordings.size,
            if (skipped > 0) ", skipped $skipped view(s) with no matching events" else "")
    }

    /** Returns true when the view was written, false when the recording holds no such event. */
    private fun runJfrconv(jfrconv: String, recording: File, output: File, type: String,
                           extraArgs: List<String>, title: String): Boolean {
        val stderr = ByteArrayOutputStream()
        val result = execOperations.exec {
            commandLine(buildList {
                add(jfrconv)
                add("--$type")
                addAll(extraArgs)
                add("--title")
                add(title)
                add(recording.absolutePath)
                add(output.absolutePath)
            })
            errorOutput = stderr
            standardOutput = ByteArrayOutputStream()
            isIgnoreExitValue = true
        }
        if (result.exitValue == 0) {
            return true
        }
        // jfrconv leaves a truncated file behind when it fails, which would look like a real view.
        output.delete()
        logger.info("Skipping the {} view of {}: {}", type, recording.name,
            stderr.toString().trim().ifEmpty { "jfrconv exited with ${result.exitValue}" })
        return false
    }
}
