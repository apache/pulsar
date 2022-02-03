/**
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
package org.apache.pulsar.common.policies.data;

/**
 * Pulsar Schema compatibility strategy.
 */
public enum SchemaCompatibilityStrategy {

    /**
     * Undefined.
     */
    UNDEFINED,

    /**
     * Always incompatible.
     */
    ALWAYS_INCOMPATIBLE,

    /**
     * Always compatible.
     */
    ALWAYS_COMPATIBLE,

    /**
     * Messages written by an old schema can be read by a new schema.
     */
    BACKWARD,

    /**
     * Messages written by a new schema can be read by an old schema.
     */
    FORWARD,

    /**
     * Equivalent to both FORWARD and BACKWARD.
     */
    FULL,

    /**
     * Be similar to BACKWARD, BACKWARD_TRANSITIVE ensure all previous version schema can
     * be read by the new schema.
     */
    BACKWARD_TRANSITIVE,

    /**
     * Be similar to FORWARD, FORWARD_TRANSITIVE ensure new schema can be ready by all previous
     * version schema.
     */
    FORWARD_TRANSITIVE,

    /**
     * Equivalent to both FORWARD_TRANSITIVE and BACKWARD_TRANSITIVE.
     */
    FULL_TRANSITIVE;


    public static boolean isUndefined(SchemaCompatibilityStrategy strategy) {
        return strategy == null || strategy == SchemaCompatibilityStrategy.UNDEFINED;
    }

    public static SchemaCompatibilityStrategy fromAutoUpdatePolicy(SchemaAutoUpdateCompatibilityStrategy strategy) {
        if (strategy == null) {
            return null;
        }
        switch (strategy) {
            case Backward:
                return BACKWARD;
            case Forward:
                return FORWARD;
            case Full:
                return FULL;
            case AlwaysCompatible:
                return ALWAYS_COMPATIBLE;
            case ForwardTransitive:
                return FORWARD_TRANSITIVE;
            case BackwardTransitive:
                return BACKWARD_TRANSITIVE;
            case FullTransitive:
                return FULL_TRANSITIVE;
            case AutoUpdateDisabled:
            default:
                return ALWAYS_INCOMPATIBLE;
        }
    }
}
