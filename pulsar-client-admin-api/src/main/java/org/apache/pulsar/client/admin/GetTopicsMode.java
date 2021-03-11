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

package org.apache.pulsar.client.admin;

/**
 * Pulsar supports persistent topic and non-persistent topic.
 * The GetTopicsMode is used to get persistent/non-persistent topics.
 */
public enum GetTopicsMode {

    /**
     * Get persistent topics.
     */
    PERSISTENT("persistent"),

    /**
     * Get non-persistent topics.
     */
    NON_PERSISTENT("non-persistent"),

    /**
     * Get both persistent and non-persistent topics.
     */
    ALL("all");

    private final String value;

    GetTopicsMode(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public static GetTopicsMode of(String value) {
        for (GetTopicsMode e : values()) {
            if (e.value.equalsIgnoreCase(value)) {
                return e;
            }
        }
        throw new IllegalArgumentException("Invalid get topic mode: [" + value + "]");
    }
}
