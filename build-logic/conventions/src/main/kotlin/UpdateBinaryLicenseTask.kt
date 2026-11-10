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
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.UntrackedTask
import java.io.File
import javax.inject.Inject

/**
 * Updates the versions of the jars that the LICENSE and NOTICE source files of a binary
 * distribution reference, after a dependency upgrade changed the jars the distribution bundles.
 *
 * One version change in the catalog can change many bundled jars, for example every Netty
 * module and its native classifiers, or the transitive dependencies of the upgraded library.
 * Each reference that no longer matches a bundled jar is replaced with the bundled jar that is
 * the same artifact at another version (see [BinaryLicenseSupport.matchVersionChanges]).
 *
 * The task does not add or remove entries: a jar that is bundled for the first time needs its
 * license reviewed, and an entry for a jar that is no longer bundled may be the last one under
 * its license heading. Both are reported, after the version changes have been written, and the
 * task fails so that they are not missed. Run `checkBinaryLicense` afterwards to verify.
 */
@UntrackedTask(because = "Rewrites the LICENSE and NOTICE source files in place")
abstract class UpdateBinaryLicenseTask : DefaultTask() {

    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val binaryDistribution: RegularFileProperty

    /** The LICENSE source file that the distribution ships as LICENSE. */
    @get:Internal
    abstract val licenseFile: RegularFileProperty

    /** The NOTICE source file that the distribution ships as NOTICE. */
    @get:Internal
    abstract val noticeFile: RegularFileProperty

    @get:Inject
    abstract val archiveOperations: ArchiveOperations

    @TaskAction
    fun update() {
        val bundledJars = BinaryLicenseSupport.readDistribution(
            archiveOperations,
            binaryDistribution.get().asFile,
        ).bundledJars
        val problems = mutableListOf<String>()

        val license = licenseFile.get().asFile
        val licenseText = license.readText()
        val unlicensedJars = bundledJars.filter {
            BinaryLicenseSupport.needsLicenseEntry(it) && !licenseText.contains(it)
        }
        val staleLicenseReferences = BinaryLicenseSupport.extractJarReferences(licenseText).distinct()
            .filterNot { BinaryLicenseSupport.isBundled(it, bundledJars) }
        val licenseMatches = BinaryLicenseSupport.matchVersionChanges(staleLicenseReferences, unlicensedJars)
        rewrite(license, licenseText, licenseMatches.renames)
        report(license.name, licenseMatches, problems)
        (unlicensedJars - licenseMatches.renames.values.toSet()).forEach {
            problems.add("$it is bundled, but has no entry in ${license.name}; add one after reviewing its license")
        }

        val notice = noticeFile.get().asFile
        val noticeText = notice.readText()
        val staleNoticeReferences = BinaryLicenseSupport.extractJarReferences(noticeText).distinct()
            .filter { it != BinaryLicenseSupport.NOTICE_EXEMPT_JAR }
            .filterNot { BinaryLicenseSupport.isBundled(it, bundledJars) }
        val noticeMatches = BinaryLicenseSupport.matchVersionChanges(staleNoticeReferences, bundledJars)
        rewrite(notice, noticeText, noticeMatches.renames)
        report(notice.name, noticeMatches, problems)

        if (problems.isNotEmpty()) {
            problems.forEach { logger.error(it) }
            throw GradleException(
                "${problems.size} LICENSE/NOTICE issue(s) need a manual edit; the version changes have been written.",
            )
        }
    }

    private fun rewrite(file: File, text: String, renames: Map<String, String>) {
        if (renames.isEmpty()) {
            logger.lifecycle("${file.name}: up to date")
            return
        }
        renames.forEach { (from, to) -> logger.lifecycle("${file.name}: $from -> $to") }
        file.writeText(BinaryLicenseSupport.replaceJarNames(text, renames))
    }

    private fun report(fileName: String, matches: JarVersionMatches, problems: MutableList<String>) {
        matches.ambiguous.forEach { (reference, candidates) ->
            problems.add("$reference in $fileName could be replaced with any of ${candidates.joinToString()}")
        }
        matches.unmatched.forEach {
            problems.add("$it is referenced in $fileName, but no longer bundled; remove or update the entry")
        }
    }
}
