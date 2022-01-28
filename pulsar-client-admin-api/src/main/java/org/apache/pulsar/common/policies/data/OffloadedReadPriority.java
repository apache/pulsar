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

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public enum OffloadedReadPriority {
    /**
     * For offloaded messages, readers will try to read from bookkeeper at first,
     * if messages not exist at bookkeeper then read from offloaded storage.
     */
    BOOKKEEPER_FIRST("bookkeeper-first"),
    /**
     * For offloaded messages, readers will try to read from offloaded storage first,
     * even they are still exist in bookkeeper.
     */
    TIERED_STORAGE_FIRST("tiered-storage-first");

    private final String value;

    OffloadedReadPriority(String value) {
        this.value = value;
    }

    public boolean equalsName(String otherName) {
        return value.equals(otherName);
    }

    @Override
    public String toString() {
        return value;
    }

    public static OffloadedReadPriority fromString(String str) {
        for (OffloadedReadPriority value : OffloadedReadPriority.values()) {
            if (value.value.equals(str)) {
                return value;
            }
        }

        throw new IllegalArgumentException("--offloadedReadPriority parameter must be one of "
                + Arrays.stream(OffloadedReadPriority.values())
                .map(OffloadedReadPriority::toString)
                .collect(Collectors.joining(","))
                + " but got: " + str);
    }

    public String getValue() {
        return value;
    }
}