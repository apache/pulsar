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
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault
import java.io.DataInputStream
import java.io.InputStream
import java.util.jar.JarFile

/** Checks bytecode as well as Gradle's JVM attributes (many Maven jars publish no JVM metadata). */
@DisableCachingByDefault(because = "Verification has no outputs")
abstract class VerifyJavaCompatibility : DefaultTask() {
    @get:Classpath
    abstract val classpath: ConfigurableFileCollection

    @get:Input
    abstract val javaVersion: Property<Int>

    @TaskAction
    fun verify() {
        val target = javaVersion.get()
        val failures = mutableListOf<String>()
        fun inspect(name: String, stream: InputStream) {
            DataInputStream(stream).use { input ->
                if (input.readInt() != 0xCAFEBABE.toInt()) {
                    throw GradleException("Invalid class file: $name")
                }
                val minor = input.readUnsignedShort()
                val major = input.readUnsignedShort()
                if (major > target + 44 || minor == 65535) {
                    failures.add("$name requires Java ${major - 44}" + if (minor == 65535) " preview" else "")
                }
            }
        }
        for (file in classpath.files) {
            if (file.isDirectory) {
                file.walkTopDown().filter { it.isFile && it.extension == "class" }.forEach {
                    inspect(it.path, it.inputStream())
                }
            } else if (file.extension == "jar") {
                JarFile(file).use { jar ->
                    val multiRelease = jar.manifest?.mainAttributes?.getValue("Multi-Release") == "true"
                    // Select the same class entries as the target JVM, not classes for newer JVMs.
                    val selected = mutableMapOf<String, Pair<Int, java.util.jar.JarEntry>>()
                    for (entry in jar.entries()) {
                        if (!entry.name.endsWith(".class")) continue
                        val match = Regex("META-INF/versions/([0-9]+)/(.*)").matchEntire(entry.name)
                        val version = match?.groupValues?.get(1)?.toInt() ?: 0
                        if (match != null && (!multiRelease || version > target)) continue
                        val name = match?.groupValues?.get(2) ?: entry.name
                        if (version >= (selected[name]?.first ?: -1)) selected[name] = version to entry
                    }
                    selected.values.forEach { (_, entry) ->
                        inspect("${file.name}!/${entry.name}", jar.getInputStream(entry))
                    }
                }
            }
        }
        if (failures.isNotEmpty()) {
            throw GradleException("Java $target compatibility violated:\n" + failures.take(30).joinToString("\n") +
                if (failures.size > 30) "\n... ${failures.size} incompatible classes in total" else "")
        }
    }
}
