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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableMap;

import com.google.common.base.MoreObjects;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Schema {
    public final SchemaVersion version;
    public final SchemaType type;
    public final boolean isDeleted;
    public final long timestamp;
    public final String user;
    public final byte[] data;
    public final Map<String, String> props;

    private Schema(Builder builder) {
        this.type = builder.type;
        this.isDeleted = builder.isDeleted;
        this.timestamp = builder.timestamp;
        this.user = builder.user;
        this.data = builder.data;
        this.version = builder.version;
        this.props = unmodifiableMap(builder.props);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schema schema = (Schema) o;
        return isDeleted == schema.isDeleted &&
            timestamp == schema.timestamp &&
            Objects.equals(version, schema.version) &&
            type == schema.type &&
            Objects.equals(user, schema.user) &&
            Arrays.equals(data, schema.data) &&
            Objects.equals(props, schema.props);
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(version, type, isDeleted, timestamp, user, props);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("version", version)
            .add("type", type)
            .add("isDeleted", isDeleted)
            .add("timestamp", timestamp)
            .add("user", user)
            .add("data", data)
            .add("props", props)
            .toString();
    }

    public static class Builder {
        private SchemaVersion version;
        private SchemaType type;
        private boolean isDeleted;
        private long timestamp;
        private String user;
        private byte[] data;
        private Map<String, String> props = new HashMap<>();

        public Builder type(SchemaType type) {
            this.type = type;
            return this;
        }

        public Builder isDeleted(boolean isDeleted) {
            this.isDeleted = isDeleted;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder data(byte[] data) {
            this.data = data;
            return this;
        }

        public Builder version(SchemaVersion version) {
            this.version = version;
            return this;
        }

        public Builder property(String key, String value) {
            this.props.put(key, value);
            return this;
        }

        public Builder properties(Map<String, String> props) {
            this.props.putAll(props);
            return this;
        }

        public Schema build() {
            checkNotNull(type);
            checkNotNull(user);
            checkNotNull(data);
            return new Schema(this);
        }

    }
}