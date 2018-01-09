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

package org.apache.pulsar.functions.fs;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;

/**
 * A identifier for an object.
 */
public class UUID implements Serializable {

    private static final long serialVersionUID = 1L;

    private final java.util.UUID uuid;

    public UUID() {
        this.uuid = java.util.UUID.randomUUID();
    }

    public UUID(long mostSigBits, long leastSigBits) {
        this.uuid = new java.util.UUID(mostSigBits, leastSigBits);
    }

    @VisibleForTesting
    java.util.UUID getInternalUUID() {
        return uuid;
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UUID)) {
            return false;
        }
        UUID another = (UUID) obj;
        return uuid.equals(another.uuid);
    }

    @Override
    public String toString() {
        return uuid.toString();
    }
}
