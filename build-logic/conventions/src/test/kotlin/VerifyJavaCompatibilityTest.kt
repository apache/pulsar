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
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.file.Files
import java.util.jar.Attributes
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import java.util.jar.Manifest

class VerifyJavaCompatibilityTest {
    private fun verify(vararg entries: Pair<String, Int>, multiRelease: Boolean = false) {
        val directory = Files.createTempDirectory("java-compatibility-test").toFile()
        try {
            val manifest = Manifest().apply {
                mainAttributes[Attributes.Name.MANIFEST_VERSION] = "1.0"
                if (multiRelease) mainAttributes.putValue("Multi-Release", "true")
            }
            val jar = directory.resolve("dependency.jar")
            JarOutputStream(jar.outputStream(), manifest).use { output ->
                for ((name, javaVersion) in entries) {
                    output.putNextEntry(JarEntry(name))
                    val bytes = ByteArrayOutputStream()
                    DataOutputStream(bytes).use {
                        it.writeInt(0xCAFEBABE.toInt())
                        it.writeShort(0)
                        it.writeShort(javaVersion + 44)
                    }
                    output.write(bytes.toByteArray())
                    output.closeEntry()
                }
            }
            val project = ProjectBuilder.builder().withProjectDir(directory).build()
            val task = project.tasks.register("verify", VerifyJavaCompatibility::class.java).get()
            task.javaVersion.set(17)
            task.classpath.from(jar)
            task.verify()
        } finally {
            directory.deleteRecursively()
        }
    }

    @Test
    fun rejectsJava21DependencyWithoutGradleMetadata() {
        val error = expectThrows(GradleException::class.java) { verify("Library.class" to 21) }
        assertTrue(error.message!!.contains("Library.class requires Java 21"))
    }

    @Test
    fun acceptsJava17WithNewerMultiReleaseImplementations() {
        verify("Library.class" to 17, "META-INF/versions/21/Library.class" to 21, multiRelease = true)
    }

    @Test
    fun rejectsIncompatibleSelectedMultiReleaseEntry() {
        expectThrows(GradleException::class.java) {
            verify("Library.class" to 8, "META-INF/versions/17/Library.class" to 21, multiRelease = true)
        }
    }

    @Test
    fun ignoresVersionedEntriesWithoutMultiReleaseManifest() {
        verify("Library.class" to 17, "META-INF/versions/17/Library.class" to 21)
    }
}
