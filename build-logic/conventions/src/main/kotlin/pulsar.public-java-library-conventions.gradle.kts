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

// Convention plugin for public Pulsar Java libraries that are published to Maven repositories.
// Combines java-conventions (compilation, testing) with publish-conventions (Maven publishing,
// signing, POM metadata), and publishes the standard `java` component with resolved version
// mapping. Shaded modules use pulsar.public-shaded-library-conventions instead. Internal-only
// modules should use pulsar.java-conventions directly.

plugins {
    id("pulsar.java-conventions")
    id("pulsar.publish-conventions")
}

publishJavaLibrary("java")
configurePulsarPublication {
    useResolvedVersions()
}

// The java component of a shaded module would publish the bundled modules as dependencies.
pluginManager.withPlugin("com.gradleup.shadow") {
    throw GradleException("Project '${project.path}' is shaded: apply pulsar.public-shaded-library-conventions " +
        "instead of pulsar.public-java-library-conventions.")
}

// Validate that public java-library modules only depend on other published modules
// in scopes that end up in the published POM (api, implementation, runtimeOnly).
validatePublishedDependencies(listOf("api", "implementation", "runtimeOnly"))
