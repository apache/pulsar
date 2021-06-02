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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The rack configuration map for bookies.
 */
public class BookiesRackConfiguration extends TreeMap<String, Map<String, BookieInfo>> {

    private static final long serialVersionUID = 0L;

    public synchronized boolean removeBookie(String address) {
        for (Map.Entry<String, Map<String, BookieInfo>> entry : entrySet()) {
            if (entry.getValue().remove(address) != null) {
                if (entry.getValue().isEmpty()) {
                    remove(entry.getKey());
                }
                return true;
            }
        }
        return false;
    }

    public synchronized Optional<BookieInfo> getBookie(String address) {
        for (Map<String, BookieInfo> m : values()) {
            BookieInfo bi = m.get(address);
            if (bi != null) {
                return Optional.of(bi);
            }
        }
        return Optional.empty();
    }

    public synchronized void updateBookie(String group, String address, BookieInfo bookieInfo) {
        Objects.requireNonNull(group);
        Objects.requireNonNull(address);
        Objects.requireNonNull(bookieInfo);

        // Remove from any group first
        removeBookie(address);
        computeIfAbsent(group, key -> new TreeMap<>()).put(address, bookieInfo);
    }
}
