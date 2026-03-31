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

// Enforced platform module that declares version constraints for all dependencies.
// This is the Gradle equivalent of Maven's dependencyManagement section.
// All subprojects consume this via: implementation(enforcedPlatform(project(":pulsar-dependencies")))
plugins {
    `java-platform`
}

group = "org.apache.pulsar"
version = the<VersionCatalogsExtension>().named("libs").findVersion("pulsar").get().requiredVersion

// Allow declaring constraints on dependencies that also appear as direct dependencies
javaPlatform {
    allowDependencies()
}

dependencies {
    val catalog = project.extensions.getByType<VersionCatalogsExtension>().named("libs")
    // Iterate over all library declarations in the version catalog and add them as constraints.
    // This ensures that any transitive dependency matching a catalog entry gets pinned to
    // the version we specify, regardless of what version a transitive dependency requests.
    catalog.libraryAliases.forEach { alias ->
        catalog.findLibrary(alias).ifPresent { provider ->
            val module = provider.get().module
            if (module.name.endsWith("-bom") || module.name.endsWith("_bom") || module.name == "bom"
                    || module.name.contains("-bom-") || module.name.contains("_bom_")) {
                api(platform(provider))
            } else {
                constraints.api(provider)
            }
        }
    }
}
