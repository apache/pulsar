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

import com.github.jengelman.gradle.plugins.shadow.relocation.CacheableRelocator
import com.github.jengelman.gradle.plugins.shadow.relocation.SimpleRelocator
import org.gradle.api.tasks.Input

/** Isolate bundled Pulsar implementations while preserving the classes owned by API modules. */
@CacheableRelocator
class PulsarImplementationRelocator : SimpleRelocator(
    "org.apache.pulsar", "org.apache.pulsar.shade.org.apache.pulsar"
) {
    @get:Input
    var publicApiPaths: Set<String> = emptySet()

    override fun canRelocatePath(path: String): Boolean {
        val normalized = path.removePrefix("/").removeSuffix(".class")
        if (!normalized.startsWith("org/apache/pulsar/")
            || normalized.startsWith("org/apache/pulsar/shade/")) {
            return false
        }
        // A set lookup also covers nested API classes, without testing hundreds of Ant
        // exclusion patterns for every constant-pool entry in the shaded dependencies.
        return normalized.substringBefore('$') !in publicApiPaths && super.canRelocatePath(path)
    }
}
