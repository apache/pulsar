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
 * After a dependency upgrade, [UpdateBinaryLicenseTask] fixes the jar versions that these checks
 * report.
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
        val contents = BinaryLicenseSupport.readDistribution(archiveOperations, tarFile)
        val bundledJars = contents.bundledJars
        val license = contents.license

        val licenseJars = BinaryLicenseSupport.extractJarReferences(license)
        val noticeJars = BinaryLicenseSupport.extractJarReferences(contents.notice)

        val errors = mutableListOf<String>()

        // Check 1: every bundled non-pulsar jar must appear as a substring of LICENSE.
        for (jar in bundledJars) {
            if (!BinaryLicenseSupport.needsLicenseEntry(jar)) continue
            if (!license.contains(jar)) {
                errors.add("$jar unaccounted for in LICENSE")
            }
        }

        // Check 2: every jar mentioned in LICENSE must be bundled.
        for (jar in licenseJars) {
            if (!BinaryLicenseSupport.isBundled(jar, bundledJars)) {
                errors.add("$jar mentioned in LICENSE, but not bundled")
            }
        }

        // Check 3: every jar mentioned in NOTICE (except checker-qual.jar) must be bundled.
        for (jar in noticeJars) {
            if (jar == BinaryLicenseSupport.NOTICE_EXEMPT_JAR) continue
            if (!BinaryLicenseSupport.isBundled(jar, bundledJars)) {
                errors.add("$jar mentioned in NOTICE, but not bundled")
            }
        }

        val reportFile = report.get().asFile
        reportFile.parentFile.mkdirs()
        reportFile.writeText(buildReport(tarFile.name, bundledJars, licenseJars, noticeJars, errors))

        if (errors.isNotEmpty()) {
            errors.forEach { logger.error(it) }
            val summary = "LICENSE/NOTICE check failed for ${tarFile.name}: ${errors.size} issue(s)."
            val howToFix = howToFix()
            logger.error(howToFix)
            if (System.getenv("GITHUB_ACTIONS") == "true") {
                // A workflow command, so that GitHub shows the fix on the pull request's checks page
                println("::error title=checkBinaryLicense failed::${escapeWorkflowCommand("$summary\n$howToFix")}")
            }
            throw GradleException("$summary See report at ${reportFile.absolutePath}.\n$howToFix")
        }
    }

    /** How to fix a failed check in a pull request, using the pull request number when in CI. */
    private fun howToFix(): String {
        val pullRequest = System.getenv("GITHUB_REF")
            ?.let { Regex("^refs/pull/(\\d+)/").find(it)?.groupValues?.get(1) }
        val checkout = if (pullRequest != null) {
            "gh pr checkout $pullRequest"
        } else {
            "gh pr checkout <pull request number>"
        }
        return """
            |How to fix: when a dependency upgrade changed the bundled jars, as in a Dependabot pull request,
            |update the LICENSE/NOTICE files on the pull request's branch and push the change to it:
            |    $checkout
            |    ./gradlew updateBinaryLicense
            |    ./gradlew checkBinaryLicense
            |    git commit -am "Update the binary LICENSE/NOTICE files" && git push
            |updateBinaryLicense updates the versions of the referenced jars. It fails for the issues that need a
            |manual edit: a jar that is bundled for the first time needs a LICENSE entry after its license has been
            |reviewed, and the entry of a jar that is no longer bundled has to be removed.
        """.trimMargin()
    }

    private fun escapeWorkflowCommand(message: String): String =
        message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")

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
