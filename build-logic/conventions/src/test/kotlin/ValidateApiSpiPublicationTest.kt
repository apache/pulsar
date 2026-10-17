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
import org.gradle.testfixtures.ProjectBuilder
import org.testng.Assert.assertTrue
import org.testng.Assert.expectThrows
import org.testng.annotations.Test
import java.nio.file.Files

class ValidateApiSpiPublicationTest {
    private fun verify(pomDependency: String = "", metadataDependency: String = "", parent: String = "") {
        val directory = Files.createTempDirectory("api-spi-publication-test").toFile()
        try {
            val project = ProjectBuilder.builder().withProjectDir(directory).build()
            val task = project.tasks.register("validate", ValidateApiSpiPublication::class.java).get()
            task.publicationGroup.set("example.pulsar")
            val pom = directory.resolve("pom.xml")
            pom.writeText("""
                <project><modelVersion>4.0.0</modelVersion>
                $parent
                <groupId>example.pulsar</groupId><artifactId>client</artifactId><version>1</version>
                <dependencies>$pomDependency</dependencies></project>
            """.trimIndent())
            val module = directory.resolve("module.json")
            module.writeText("""
                {"component":{"group":"example.pulsar","module":"client","version":"1"},
                "variants":[{"name":"runtime","dependencies":[$metadataDependency]}]}
            """.trimIndent())
            task.poms.from(pom)
            task.moduleMetadata.from(module)
            task.validate()
        } finally {
            directory.deleteRecursively()
        }
    }

    @Test
    fun acceptsClosedGraphAndThirdPartyDependencies() {
        verify(
            "<dependency><groupId>example.pulsar</groupId><artifactId>client</artifactId>" +
                "<version>1</version></dependency>",
            """{"group":"third.party","module":"library","version":{"requires":"2"}}"""
        )
    }

    @Test
    fun rejectsMissingPomDependency() {
        val error = expectThrows(GradleException::class.java) {
            verify("<dependency><groupId>example.pulsar</groupId><artifactId>forgotten</artifactId>" +
                "<version>1</version></dependency>")
        }
        assertTrue(error.message!!.contains("client (pom.xml) -> example.pulsar:forgotten:1"))
    }

    @Test
    fun rejectsMissingMetadataDependency() {
        val error = expectThrows(GradleException::class.java) {
            verify(metadataDependency =
                """{"group":"example.pulsar","module":"forgotten","version":{"requires":"1"}}""")
        }
        assertTrue(error.message!!.contains("client (Gradle metadata) -> example.pulsar:forgotten:1"))
    }

    @Test
    fun rejectsOriginalGroupAndMissingParent() {
        val error = expectThrows(GradleException::class.java) {
            verify(parent = "<parent><groupId>org.apache.pulsar</groupId><artifactId>pulsar</artifactId>" +
                "<version>1</version></parent>")
        }
        assertTrue(error.message!!.contains("org.apache.pulsar:pulsar:1"))
    }

    @Test
    fun rejectsWrongInternalVersion() {
        val error = expectThrows(GradleException::class.java) {
            verify(metadataDependency =
                """{"group":"example.pulsar","module":"client","version":{"strictly":"2"}}""")
        }
        assertTrue(error.message!!.contains("example.pulsar:client:2"))
    }
}
