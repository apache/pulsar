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

import org.testng.Assert.assertEquals
import org.testng.Assert.assertTrue
import org.testng.annotations.Test

class BinaryLicenseSupportTest {

    @Test
    fun matchesEveryJarOfAVersionChange() {
        val matches = BinaryLicenseSupport.matchVersionChanges(
            listOf(
                "io.netty-netty-codec-4.2.20.Final.jar",
                "io.netty-netty-codec-http-4.2.20.Final.jar",
                "io.netty-netty-transport-native-epoll-4.2.20.Final-linux-x86_64.jar",
                "io.netty-netty-transport-native-epoll-4.2.20.Final-linux-aarch_64.jar",
            ),
            listOf(
                "io.netty-netty-codec-http-4.2.21.Final.jar",
                "io.netty-netty-transport-native-epoll-4.2.21.Final-linux-aarch_64.jar",
                "io.netty-netty-codec-4.2.21.Final.jar",
                "io.netty-netty-transport-native-epoll-4.2.21.Final-linux-x86_64.jar",
            ),
        )
        assertEquals(
            matches.renames,
            mapOf(
                "io.netty-netty-codec-4.2.20.Final.jar" to "io.netty-netty-codec-4.2.21.Final.jar",
                "io.netty-netty-codec-http-4.2.20.Final.jar" to "io.netty-netty-codec-http-4.2.21.Final.jar",
                "io.netty-netty-transport-native-epoll-4.2.20.Final-linux-x86_64.jar" to
                    "io.netty-netty-transport-native-epoll-4.2.21.Final-linux-x86_64.jar",
                "io.netty-netty-transport-native-epoll-4.2.20.Final-linux-aarch_64.jar" to
                    "io.netty-netty-transport-native-epoll-4.2.21.Final-linux-aarch_64.jar",
            ),
        )
        assertTrue(matches.ambiguous.isEmpty())
        assertTrue(matches.unmatched.isEmpty())
    }

    @Test
    fun keepsAlphaAndStableArtifactsApart() {
        val matches = BinaryLicenseSupport.matchVersionChanges(
            listOf("opentelemetry-api-1.67.0.jar", "opentelemetry-api-incubator-1.67.0-alpha.jar"),
            listOf("opentelemetry-api-incubator-1.67.1-alpha.jar", "opentelemetry-api-1.67.1.jar"),
        )
        assertEquals(
            matches.renames,
            mapOf(
                "opentelemetry-api-1.67.0.jar" to "opentelemetry-api-1.67.1.jar",
                "opentelemetry-api-incubator-1.67.0-alpha.jar" to "opentelemetry-api-incubator-1.67.1-alpha.jar",
            ),
        )
    }

    @Test
    fun handlesDigitsInTheArtifactNameAndChangedVersionShapes() {
        val matches = BinaryLicenseSupport.matchVersionChanges(
            listOf("log4j-1.2-api-2.26.0.jar", "bcprov-jdk18on-1.80.jar", "zstd-jni-1.5.7-6.jar"),
            listOf("log4j-api-2.26.1.jar", "log4j-1.2-api-2.26.1.jar", "bcprov-jdk18on-1.80.1.jar", "zstd-jni-1.5.7-7.jar"),
        )
        assertEquals(
            matches.renames,
            mapOf(
                "log4j-1.2-api-2.26.0.jar" to "log4j-1.2-api-2.26.1.jar",
                "bcprov-jdk18on-1.80.jar" to "bcprov-jdk18on-1.80.1.jar",
                "zstd-jni-1.5.7-6.jar" to "zstd-jni-1.5.7-7.jar",
            ),
        )
    }

    @Test
    fun leavesNewRemovedAndAmbiguousJarsToAHuman() {
        val matches = BinaryLicenseSupport.matchVersionChanges(
            listOf("removed-1.0.jar", "foo-1.0.jar"),
            listOf("brand-new-2.0.jar", "foo-1.1-a.jar", "foo-1.1-b.jar"),
        )
        assertTrue(matches.renames.isEmpty())
        assertEquals(matches.unmatched, listOf("removed-1.0.jar"))
        assertEquals(matches.ambiguous, mapOf("foo-1.0.jar" to listOf("foo-1.1-a.jar", "foo-1.1-b.jar")))
    }

    @Test
    fun doesNotGiveOneJarToTwoReferences() {
        val matches = BinaryLicenseSupport.matchVersionChanges(
            listOf("foo-1.0.jar", "foo-1.1.jar"),
            listOf("foo-1.2.jar"),
        )
        assertTrue(matches.renames.isEmpty())
        assertEquals(matches.ambiguous.keys, setOf("foo-1.0.jar", "foo-1.1.jar"))
    }

    @Test
    fun replacesWholeJarNamesOnly() {
        val text = """
            | * Netty -- io.netty-netty-codec-4.2.20.Final.jar
            | * Netty HTTP -- io.netty-netty-codec-http-4.2.20.Final.jar
        """.trimMargin()
        val updated = BinaryLicenseSupport.replaceJarNames(
            text,
            mapOf("io.netty-netty-codec-4.2.20.Final.jar" to "io.netty-netty-codec-4.2.21.Final.jar"),
        )
        assertEquals(
            updated,
            """
                | * Netty -- io.netty-netty-codec-4.2.21.Final.jar
                | * Netty HTTP -- io.netty-netty-codec-http-4.2.20.Final.jar
            """.trimMargin(),
        )
    }
}
