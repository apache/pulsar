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
package org.apache.pulsar.common.schema;

import com.google.common.base.MoreObjects;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

/**
 * Long schema version.
 */
public class LongSchemaVersion implements SchemaVersion {
    private final long version;

    public LongSchemaVersion(long version) {
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public byte[] bytes() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(version);
        buffer.rewind();
        return buffer.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongSchemaVersion that = (LongSchemaVersion) o;
        return version == that.version;
    }

    @Override
    public int hashCode() {

        return Objects.hash(version);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("version", version)
            .toString();
    }
}
