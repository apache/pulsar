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

@file:Suppress("UnstableApiUsage")
import java.net.URLClassLoader

// Convention plugin for lightproto code generation.
// lightproto v0.4 has no official Gradle plugin, so we invoke
// the code generator library via a custom task using reflection.

plugins {
    java
}

val lightprotoVersion = "0.4"

val lightprotoConfig = configurations.create("lightproto") {
    isCanBeConsumed = false
    isCanBeResolved = true
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    lightprotoConfig("com.github.splunk.lightproto:lightproto-code-generator:$lightprotoVersion")
}

val lightprotoInputDir = layout.projectDirectory.dir("src/main/proto")
val lightprotoOutputDir = layout.buildDirectory.dir("generated/source/lightproto/java")

val generateLightproto = tasks.register("generateLightproto") {
    description = "Generate Java sources from .proto files using lightproto"
    group = "build"

    inputs.dir(lightprotoInputDir)
    outputs.dir(lightprotoOutputDir)

    doLast {
        val inputDir = lightprotoInputDir.asFile
        val outputDir = lightprotoOutputDir.get().asFile
        outputDir.mkdirs()

        if (!inputDir.exists() || inputDir.listFiles()?.isEmpty() != false) {
            logger.info("No .proto files found in $inputDir, skipping lightproto generation")
            return@doLast
        }

        // Collect proto files (filtered by lightprotoIncludes property if set)
        val includes = project.findProperty("lightprotoIncludes") as? String
        val protoFiles = if (includes != null) {
            includes.split(",").map { File(it.trim()) }.filter { it.exists() }
        } else {
            inputDir.walkTopDown().filter { it.extension == "proto" }.toList()
        }

        if (protoFiles.isEmpty()) {
            logger.info("No .proto files found, skipping lightproto generation")
            return@doLast
        }

        // Use a classloader to invoke LightProtoGenerator.generate()
        @Suppress("UNCHECKED_CAST")
        val urls = lightprotoConfig.files.map { it.toURI().toURL() }.toTypedArray()
        val classLoader = URLClassLoader(urls, Thread.currentThread().contextClassLoader)

        val generatorClass = classLoader.loadClass("com.github.splunk.lightproto.generator.LightProtoGenerator")
        val generateMethod = generatorClass.methods.first { it.name == "generate" }

        logger.lifecycle("Generating lightproto sources from ${protoFiles.size} .proto file(s)")
        generateMethod.invoke(null, protoFiles, outputDir, "", false)
    }
}

sourceSets.main {
    java.srcDir(lightprotoOutputDir.map { it.asFile })
}

tasks.named("compileJava") {
    dependsOn(generateLightproto)
}
