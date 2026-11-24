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

import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property

/**
 * Configuration for the `pulsar.minimized-dependencies-conventions` plugin.
 *
 * A "minimized" packaging module declares its reachability roots as `api(project(...))`
 * dependencies and then only needs to express which libraries to minimize and the
 * expected upper bound on the retained class count.
 */
interface MinimizedDependenciesExtension {
    /** Libraries to minimize, as `"group:name"` entries (e.g. `"it.unimi.dsi:fastutil"`). */
    val minimizedDependencies: ListProperty<String>

    /**
     * Upper bound on the number of `.class` entries retained in the shaded jar. The build fails
     * if the jar exceeds it — this catches a minimize() regression that would ship the full jar.
     */
    val maxRetainedClasses: Property<Int>
}
