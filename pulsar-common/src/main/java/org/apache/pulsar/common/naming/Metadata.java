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

public class Metadata {

    private static final int MAX_KEY_VALUE_PAIRS = 8;
    private static final int MAX_KEY_OR_VALUE_LENGTH = 63;

    private Metadata() {}

    public static boolean validateMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return true;
        }

        if (metadata.size() > MAX_KEY_VALUE_PAIRS) {
            return false;
        }

        for (Map.Entry<String, String> e : metadata.entrySet()) {
            if (e.getKey().length() > MAX_KEY_OR_VALUE_LENGTH || e.getValue().length() > MAX_KEY_OR_VALUE_LENGTH) {
                return false;
            }
        }

        return true;
    }

    public static String getErrorMessage() {
        return "metadata can only have " + MAX_KEY_VALUE_PAIRS
                + " key value pairs and their length must be less than "
                + (MAX_KEY_OR_VALUE_LENGTH + 1) + " characters";
    }
}
