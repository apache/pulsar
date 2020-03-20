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
package org.apache.pulsar.common.protocol.schema;

import com.google.common.base.MoreObjects;
import java.util.Arrays;
import java.util.Objects;

/**
 * Stored schema with version.
 */
public class StoredSchema {
    public final byte[] data;
    public final SchemaVersion version;

    public StoredSchema(byte[] data, SchemaVersion version) {
        this.data = data;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoredSchema that = (StoredSchema) o;
        return Arrays.equals(data, that.data)
                && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(version);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("data", data)
            .add("version", version)
            .toString();
    }
}
