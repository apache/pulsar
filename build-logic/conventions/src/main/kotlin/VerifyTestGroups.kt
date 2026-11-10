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
import org.gradle.api.DefaultTask
import org.gradle.api.provider.MapProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

/**
 * Scans the modules' test classes and reports their TestNG group assignments: the classes in more than
 * one CI group, and the pulsar-broker classes that no CI broker group runs.
 */
@DisableCachingByDefault(because = "Prints a report and produces no outputs")
abstract class VerifyTestGroups : DefaultTask() {

    /** The test source directory of each module to scan, by module name. Missing directories are skipped. */
    @get:Input
    abstract val testSourceDirs: MapProperty<String, String>

    @TaskAction
    fun verify() {
        // CI group definitions matching build/run_unit_group.sh
        val brokerCiGroups = mapOf(
            "BROKER_GROUP_1" to setOf("broker", "broker-isolated"),
            "BROKER_GROUP_2" to setOf("schema", "utils", "functions-worker", "broker-io",
                "broker-discovery", "broker-compaction", "broker-naming", "websocket", "other",
                "builtin", "stats"),
            "BROKER_GROUP_3" to setOf("broker-admin", "broker-admin-isolated"),
            "BROKER_GROUP_4" to setOf("cluster-migration"),
            "BROKER_GROUP_5" to setOf("broker-replication"),
            "BROKER_CLIENT_API" to setOf("broker-api"),
            "BROKER_CLIENT_IMPL" to setOf("broker-impl"),
            "BROKER_FLAKY" to setOf("quarantine", "flaky"),
        )
        val specialGroups = setOf("quarantine", "flaky")
        val groupPattern = Regex("""@Test\s*\(\s*[^)]*groups\s*=\s*(?:\{([^}]*)\}|"([^"]*)")""")

        var totalClasses = 0
        val groupCounts = mutableMapOf<String, Int>()
        val issues = mutableListOf<String>()

        for ((moduleName, testDirPath) in testSourceDirs.get()) {
            val testDir = File(testDirPath)
            if (!testDir.isDirectory) {
                continue
            }
            testDir.walkTopDown()
                .filter { it.isFile && it.name.endsWith("Test.java") }
                .forEach { file ->
                    val content = file.readText()
                    if (!content.contains("@Test")) return@forEach
                    totalClasses++

                    // Extract groups from @Test annotations
                    val groups = mutableSetOf<String>()
                    for (match in groupPattern.findAll(content)) {
                        val groupStr = match.groupValues[1].ifEmpty { match.groupValues[2] }
                        groupStr.split(",")
                            .map { it.trim().removeSurrounding("\"").trim() }
                            .filter { it.isNotEmpty() }
                            .forEach { groups.add(it) }
                    }

                    if (groups.isEmpty()) {
                        // AnnotationListener assigns "other" at runtime
                        groups.add("other")
                    }

                    groups.forEach { g ->
                        groupCounts[g] = (groupCounts[g] ?: 0) + 1
                    }

                    // Check for tests in multiple non-special groups
                    val nonSpecialGroups = groups - specialGroups
                    val relativePath = file.relativeTo(testDir.parentFile.parentFile)
                    if (nonSpecialGroups.size > 1) {
                        issues.add("$moduleName/$relativePath is in multiple groups: $nonSpecialGroups")
                    }

                    // For broker module, check CI coverage
                    if (moduleName == "pulsar-broker" && (groups intersect specialGroups).isEmpty()) {
                        val coveredByCi = brokerCiGroups.values.any { ciGroups ->
                            (groups intersect ciGroups).isNotEmpty()
                        }
                        if (!coveredByCi) {
                            issues.add("$moduleName/$relativePath groups=$groups not covered by any CI broker group")
                        }
                    }
                }
        }

        println("\n=== TestNG Group Verification Report ===\n")
        println("Total test classes scanned: $totalClasses")
        println("\nGroup distribution:")
        groupCounts.entries.sortedByDescending { it.value }.forEach { (group, count) ->
            val inCi = brokerCiGroups.entries.firstOrNull { group in it.value }?.key
            val ciLabel = if (inCi != null) " [$inCi]" else ""
            println("  %-25s %4d tests%s".format(group, count, ciLabel))
        }

        if (issues.isNotEmpty()) {
            println("\nIssues found (${issues.size}):")
            issues.forEach { println("  - $it") }
        } else {
            println("\nNo issues found")
        }

        println()
    }
}
