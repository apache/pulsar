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
package org.apache.pulsar.metadata.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helpers for extracting typed values from a {@code Set<Option>}.
 *
 * <p>{@link MetadataStore} implementations and wrappers use these to consult only the options
 * they care about, ignoring the rest.
 */
public final class OptionsHelper {

    private OptionsHelper() {}

    public static boolean isEphemeral(Set<Option> opts) {
        return opts != null && opts.contains(Option.Ephemeral.INSTANCE);
    }

    public static boolean isSequential(Set<Option> opts) {
        return opts != null && opts.contains(Option.Sequential.INSTANCE);
    }

    /** @return the partition-key value, or {@code null} if no {@link Option.PartitionKey} is present. */
    public static String partitionKey(Set<Option> opts) {
        if (opts == null) {
            return null;
        }
        for (Option o : opts) {
            if (o instanceof Option.PartitionKey pk) {
                return pk.key();
            }
        }
        return null;
    }

    /**
     * Build the {@code indexName -> secondaryKey} map from any {@link Option.SecondaryIndex} entries.
     * Returns an empty map when no entries are present.
     */
    public static Map<String, String> secondaryIndexes(Set<Option> opts) {
        if (opts == null || opts.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> map = null;
        for (Option o : opts) {
            if (o instanceof Option.SecondaryIndex si) {
                if (map == null) {
                    map = new HashMap<>();
                }
                map.put(si.indexName(), si.secondaryKey());
            }
        }
        return map == null ? Collections.emptyMap() : map;
    }

    /**
     * @return the per-dimension increments from a {@link Option.SequenceKeysDeltas} entry, or
     *     {@code null} if no such option is present.
     */
    public static List<Long> sequenceKeysDeltas(Set<Option> opts) {
        if (opts == null) {
            return null;
        }
        for (Option o : opts) {
            if (o instanceof Option.SequenceKeysDeltas sk) {
                return sk.deltas();
            }
        }
        return null;
    }
}
