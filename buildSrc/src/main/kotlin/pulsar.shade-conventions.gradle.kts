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

plugins {
    java
    id("com.gradleup.shadow")
}

group = "org.apache.pulsar"
version = rootProject.version

repositories {
    mavenCentral()
}

// Access the version catalog for BOMs
val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

dependencies {
    // Import BOMs so version-less dependencies resolve correctly
    implementation(platform(libs.netty.bom))
    implementation(platform(libs.jackson.bom))
    implementation(platform(libs.grpc.bom))
    implementation(platform(libs.slf4j.bom))
    implementation(platform(libs.log4j.bom))
    implementation(platform(libs.mockito.bom))
    implementation(platform(libs.prometheus.bom))
    implementation(platform(libs.jjwt.bom))
}

tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
    isZip64 = true

    // Common exclusions for all shaded jars
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    exclude("**/module-info.class")

    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
            "Multi-Release" to "true",
        )
    }
}

// Disable the default jar task — shadowed jar replaces it
tasks.jar {
    enabled = false
}

// Ensure shadowJar runs after all project dependencies' shadowJars
tasks.shadowJar {
    // Resolve implicit dependencies on other shadow jars
    project.configurations.getByName("runtimeClasspath").allDependencies
        .withType<ProjectDependency>()
        .forEach { dep ->
            val depProject = dep.dependencyProject
            depProject.tasks.findByName("shadowJar")?.let { shadowTask ->
                dependsOn(shadowTask)
            }
        }
}

// Make build depend on shadowJar
tasks.named("build") {
    dependsOn(tasks.shadowJar)
}
