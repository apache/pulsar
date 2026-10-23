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

import groovy.json.JsonSlurper
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault
import org.w3c.dom.Element
import javax.xml.parsers.DocumentBuilderFactory

/** Checks the consumer graph, including publication-only rewrites and dependency-reduced shaded POMs. */
@DisableCachingByDefault(because = "Verification has no outputs")
abstract class ValidateApiSpiPublication : DefaultTask() {
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val poms: ConfigurableFileCollection

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val moduleMetadata: ConfigurableFileCollection

    @get:Input
    abstract val publicationGroup: Property<String>

    @TaskAction
    fun validate() {
        val factory = DocumentBuilderFactory.newInstance()
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        fun Element.childText(name: String): String = (0 until childNodes.length)
            .map { childNodes.item(it) }.filterIsInstance<Element>()
            .firstOrNull { it.tagName == name }?.textContent.orEmpty()
        val documents = poms.files.sorted().associateWith {
            factory.newDocumentBuilder().parse(it).documentElement
        }
        val coordinates = documents.values.map {
            "${it.childText("groupId")}:${it.childText("artifactId")}:${it.childText("version")}"
        }.toSet()
        val failures = sortedSetOf<String>()
        fun check(source: String, group: String, artifact: String, version: String) {
            if (group == publicationGroup.get() || group == "org.apache.pulsar") {
                val coordinate = "$group:$artifact:$version"
                if (coordinate !in coordinates) {
                    failures.add("$source -> $coordinate")
                }
            }
        }
        for ((file, root) in documents) {
            val source = root.childText("artifactId") + " (" + file.name + ")"
            if (root.childText("groupId") != publicationGroup.get()) {
                failures.add("$source publishes under ${root.childText("groupId")} instead of ${publicationGroup.get()}")
            }
            for (tag in listOf("parent", "dependency")) {
                val nodes = root.getElementsByTagName(tag)
                for (i in 0 until nodes.length) {
                    val node = nodes.item(i) as Element
                    check(source, node.childText("groupId"), node.childText("artifactId"), node.childText("version"))
                }
            }
        }
        for (file in moduleMetadata.files.sorted()) {
            val json = JsonSlurper().parse(file) as Map<*, *>
            val component = json["component"] as Map<*, *>
            val source = "${component["module"]} (Gradle metadata)"
            for (variant in json["variants"] as List<*>) {
                val data = variant as Map<*, *>
                for (key in listOf("dependencies", "dependencyConstraints")) {
                    for (entry in data[key] as? List<*> ?: emptyList<Any>()) {
                        val dep = entry as Map<*, *>
                        val version = dep["version"] as? Map<*, *>
                        check(source, dep["group"].toString(), dep["module"].toString(),
                            (version?.get("strictly") ?: version?.get("requires") ?: version?.get("prefers")).toString())
                    }
                }
                (data["available-at"] as? Map<*, *>)?.let {
                    check(source, it["group"].toString(), it["module"].toString(), it["version"].toString())
                }
            }
        }
        if (failures.isNotEmpty()) {
            throw GradleException("API/SPI publication contains unpublished Pulsar dependencies:\n" +
                failures.joinToString("\n") +
                "\nAdd the missing projects to PulsarApiSpiPublication.projects or correct their published coordinates.")
        }
        logger.lifecycle("Validated {} API/SPI publications and their published dependency closure.", coordinates.size)
    }
}
