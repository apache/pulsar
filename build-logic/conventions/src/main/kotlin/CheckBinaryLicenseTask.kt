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
import org.gradle.api.file.ArchiveOperations
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import javax.inject.Inject

/**
 * Checks LICENSE/NOTICE coverage of bundled jars in a binary distribution tarball.
 *
 * Mirrors the behaviour of the legacy `src/check-binary-license.sh`:
 *  1. Every bundled jar whose basename does not contain "org.apache.pulsar"
 *     must appear as a substring of the LICENSE text.
 *  2. Every jar referenced from LICENSE must be bundled.
 *  3. Every jar referenced from NOTICE (except "checker-qual.jar") must be bundled.
 *
 * Cacheable + configuration-cache friendly: state is held only on inputs/outputs and the
 * injected `ArchiveOperations` service; the task action does not reach into the project.
 */
@CacheableTask
abstract class CheckBinaryLicenseTask : DefaultTask() {

    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val binaryDistribution: RegularFileProperty

    @get:OutputFile
    abstract val report: RegularFileProperty

    @get:Inject
    abstract val archiveOperations: ArchiveOperations

    @TaskAction
    fun check() {
        val tarFile = binaryDistribution.get().asFile
        val tarTree = archiveOperations.tarTree(tarFile)

        val licenseEntryRegex = Regex("^[^/]+/LICENSE$")
        val noticeEntryRegex = Regex("^[^/]+/NOTICE$")
        val nameExclusionSubstrings = listOf(
            "pulsar-client",
            "pulsar-cli-utils",
            "pulsar-common",
            "pulsar-package",
            "pulsar-websocket",
            "bouncy-castle-bc",
        )

        val bundledJars = sortedSetOf<String>()
        var licenseContent: String? = null
        var noticeContent: String? = null

        tarTree.visit {
            if (isDirectory) return@visit
            val path = relativePath.pathString
            when {
                path.endsWith(".jar") -> {
                    val inExcludedDir = path.contains("/examples/") || path.contains("/instances/")
                    val nameExcluded = nameExclusionSubstrings.any { name.contains(it) }
                    if (!inExcludedDir && !nameExcluded) {
                        bundledJars.add(name)
                    }
                }
                licenseEntryRegex.matches(path) -> licenseContent = file.readText()
                noticeEntryRegex.matches(path) -> noticeContent = file.readText()
            }
        }

        val license = licenseContent
            ?: throw GradleException("Could not find a top-level LICENSE entry in ${tarFile.name}")
        val notice = noticeContent
            ?: throw GradleException("Could not find a top-level NOTICE entry in ${tarFile.name}")

        val licenseJars = extractJarReferences(license)
        val noticeJars = extractJarReferences(notice)

        val errors = mutableListOf<String>()

        // Check 1: every bundled non-pulsar jar must appear as a substring of LICENSE.
        for (jar in bundledJars) {
            if (jar.contains("org.apache.pulsar")) continue
            if (!license.contains(jar)) {
                errors.add("$jar unaccounted for in LICENSE")
            }
        }

        // Check 2: every jar mentioned in LICENSE must be bundled.
        // Reference may contain wildcards like "org.rocksdb.*.jar"; treat it as a regex
        // to match the legacy bash `grep -q $J` semantics.
        for (jar in licenseJars) {
            val pattern = Regex(jar)
            if (bundledJars.none { pattern.containsMatchIn(it) }) {
                errors.add("$jar mentioned in LICENSE, but not bundled")
            }
        }

        // Check 3: every jar mentioned in NOTICE (except checker-qual.jar) must be bundled.
        for (jar in noticeJars) {
            if (jar == "checker-qual.jar") continue
            val pattern = Regex(jar)
            if (bundledJars.none { pattern.containsMatchIn(it) }) {
                errors.add("$jar mentioned in NOTICE, but not bundled")
            }
        }

        val reportFile = report.get().asFile
        reportFile.parentFile.mkdirs()
        reportFile.writeText(buildReport(tarFile.name, bundledJars, licenseJars, noticeJars, errors))

        if (errors.isNotEmpty()) {
            errors.forEach { logger.error(it) }
            throw GradleException(
                "LICENSE/NOTICE check failed for ${tarFile.name}: ${errors.size} issue(s). " +
                    "See report at ${reportFile.absolutePath}",
            )
        }
    }

    private fun extractJarReferences(content: String): List<String> {
        val jarRegex = Regex(""".* (.*\.jar).*""")
        return content.lines().mapNotNull { line -> jarRegex.matchEntire(line)?.groupValues?.get(1) }
    }

    private fun buildReport(
        tarballName: String,
        bundledJars: Set<String>,
        licenseJars: List<String>,
        noticeJars: List<String>,
        errors: List<String>,
    ): String = buildString {
        appendLine("Binary license check report for $tarballName")
        appendLine("Bundled jars: ${bundledJars.size}")
        appendLine("Jars referenced in LICENSE: ${licenseJars.size}")
        appendLine("Jars referenced in NOTICE: ${noticeJars.size}")
        appendLine()
        if (errors.isEmpty()) {
            appendLine("Result: OK")
        } else {
            appendLine("Result: FAILED (${errors.size} issue(s))")
            errors.forEach { appendLine("  - $it") }
        }
    }
}
