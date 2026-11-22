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
import org.gradle.api.file.ArchiveOperations
import java.io.File

/** The jars bundled in a binary distribution tarball and the LICENSE and NOTICE it ships. */
internal class BinaryDistributionContents(
    val bundledJars: Set<String>,
    val license: String,
    val notice: String,
)

/**
 * The rules shared by [CheckBinaryLicenseTask] and [UpdateBinaryLicenseTask], so that the update
 * task produces exactly what the check task accepts.
 */
internal object BinaryLicenseSupport {
    private val licenseEntryRegex = Regex("^[^/]+/LICENSE$")
    private val noticeEntryRegex = Regex("^[^/]+/NOTICE$")
    private val nameExclusionSubstrings = listOf(
        "pulsar-client",
        "pulsar-cli-utils",
        "pulsar-common",
        "pulsar-http-client-api",
        "pulsar-package",
        "pulsar-tls-factory-api",
        "pulsar-websocket",
    )

    /** The NOTICE reference that does not need to be bundled. */
    const val NOTICE_EXEMPT_JAR = "checker-qual.jar"

    private val jarReferenceRegex = Regex(""".* (.*\.jar).*""")
    private const val JAR_NAME_CHARS = "A-Za-z0-9_.+-"
    private val digitRunRegex = Regex("""\d+""")

    fun readDistribution(archiveOperations: ArchiveOperations, tarFile: File): BinaryDistributionContents {
        val bundledJars = sortedSetOf<String>()
        var licenseContent: String? = null
        var noticeContent: String? = null

        archiveOperations.tarTree(tarFile).visit {
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

        return BinaryDistributionContents(
            bundledJars,
            licenseContent
                ?: throw GradleException("Could not find a top-level LICENSE entry in ${tarFile.name}"),
            noticeContent
                ?: throw GradleException("Could not find a top-level NOTICE entry in ${tarFile.name}"),
        )
    }

    /** The jar referenced on each line of a LICENSE or NOTICE text: the last one on the line. */
    fun extractJarReferences(content: String): List<String> =
        content.lines().mapNotNull { line -> jarReferenceRegex.matchEntire(line)?.groupValues?.get(1) }

    /** Whether LICENSE has to mention [jar]; the Pulsar jars themselves need no entry. */
    fun needsLicenseEntry(jar: String): Boolean = !jar.contains("org.apache.pulsar")

    /**
     * Whether a LICENSE or NOTICE reference matches a bundled jar. A reference may contain
     * wildcards like "org.rocksdb.*.jar", so it is treated as a regex, matching the legacy bash
     * `grep -q $J` semantics.
     */
    fun isBundled(reference: String, bundledJars: Collection<String>): Boolean {
        val pattern = Regex(reference)
        return bundledJars.any { pattern.containsMatchIn(it) }
    }

    /** Replaces each whole-word occurrence of a key of [renames] in [content] with its value. */
    fun replaceJarNames(content: String, renames: Map<String, String>): String =
        renames.entries.fold(content) { text, (from, to) ->
            Regex("(?<![$JAR_NAME_CHARS])${Regex.escape(from)}(?![$JAR_NAME_CHARS])")
                .replace(text, Regex.escapeReplacement(to))
        }

    /**
     * Pairs each [stale] reference, which no longer matches a bundled jar, with the jar in
     * [candidates] that is the same artifact at another version.
     *
     * A candidate is the same artifact when both names share a prefix that ends with "-" and is
     * followed by a digit in both, as in "netty-codec-" + "4.2.20.Final.jar". Of these, the
     * candidate whose remainder has the same shape, with each run of digits treated as equal, is
     * chosen, which keeps classifiers apart: "...-4.2.21.Final-linux-x86_64.jar" replaces
     * "...-4.2.20.Final-linux-x86_64.jar" and not "...-4.2.20.Final-linux-aarch_64.jar". When no
     * candidate has the same shape, as when "1.80.jar" becomes "1.80.1.jar", a single candidate
     * is still taken. Anything else is left for a human to resolve.
     */
    fun matchVersionChanges(stale: Collection<String>, candidates: Collection<String>): JarVersionMatches {
        val chosen = mutableMapOf<String, String>()
        val ambiguous = mutableMapOf<String, List<String>>()
        val unmatched = mutableListOf<String>()

        for (reference in stale) {
            val sameArtifact = candidates.mapNotNull { jar ->
                versionStart(reference, jar)?.let { start -> jar to start }
            }
            val sameShape = sameArtifact.filter { (jar, start) ->
                shape(reference.substring(start)) == shape(jar.substring(start))
            }
            val match = sameShape.singleOrNull() ?: if (sameShape.isEmpty()) sameArtifact.singleOrNull() else null
            when {
                match != null -> chosen[reference] = match.first
                sameArtifact.isEmpty() -> unmatched.add(reference)
                else -> ambiguous[reference] = (sameShape.ifEmpty { sameArtifact }).map { it.first }
            }
        }

        // A jar can replace only one reference; when several claim it, a human has to decide.
        val claimedTwice = chosen.values.groupingBy { it }.eachCount().filterValues { it > 1 }.keys
        val renames = chosen.filterValues { it !in claimedTwice }
        chosen.filterValues { it in claimedTwice }.forEach { (reference, jar) -> ambiguous[reference] = listOf(jar) }

        return JarVersionMatches(renames, ambiguous, unmatched)
    }

    /**
     * The length of the longest shared prefix of [a] and [b] that ends with "-" and is followed
     * by a digit in both, or null when there is none.
     */
    private fun versionStart(a: String, b: String): Int? {
        if (a == b) return null
        val common = a.commonPrefixWith(b).length
        for (i in common downTo 1) {
            if (a[i - 1] == '-' && i < a.length && i < b.length && a[i].isDigit() && b[i].isDigit()) {
                return i
            }
        }
        return null
    }

    private fun shape(versionAndRest: String): String = digitRunRegex.replace(versionAndRest, "#")
}

/**
 * The result of [BinaryLicenseSupport.matchVersionChanges]: the [renames] to apply, the
 * references with several possible replacements, and the references that match no candidate.
 */
internal data class JarVersionMatches(
    val renames: Map<String, String>,
    val ambiguous: Map<String, List<String>>,
    val unmatched: List<String>,
)
