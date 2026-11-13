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

// Convention plugin for public Pulsar libraries that are published as a shaded jar. Combines the
// shading conventions with publish-conventions, and publishes the Shadow plugin's dependency-reduced
// `shadow` component: the artifact is the shadow jar and the published dependencies are EXACTLY the
// `shadow` and `shadowApi` configurations (the non-bundled dependencies), not the bundled modules and
// their unshaded transitive tree. This is the Gradle equivalent of Maven's createDependencyReducedPom.
// Modules add pulsar.client-shade-conventions or pulsar.minimized-dependencies-conventions for their
// relocations and minimization.

plugins {
    id("pulsar.shadow-conventions")
    id("pulsar.publish-conventions")
}

publishJavaLibrary("shadow")

// Validate that the published, non-bundled dependencies are published modules.
validatePublishedDependencies(listOf("shadow", "shadowApi"))
