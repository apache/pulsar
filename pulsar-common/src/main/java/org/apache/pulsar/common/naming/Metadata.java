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
package org.apache.pulsar.common.naming;

import java.util.Map;

/**
 * Validator for metadata configuration.
 */
public class Metadata {

    private static final int MAX_METADATA_SIZE = 1024; // 1 Kb

    private Metadata() {}

    public static void validateMetadata(Map<String, String> metadata) throws IllegalArgumentException {
        if (metadata == null) {
            return;
        }

        int size = 0;
        for (Map.Entry<String, String> e : metadata.entrySet()) {
            size += (e.getKey().length() + e.getValue().length());
            if (size > MAX_METADATA_SIZE) {
                throw new IllegalArgumentException(getErrorMessage());
            }
        }
    }

    private static String getErrorMessage() {
        return "metadata has a max size of 1 Kb";
    }
}
