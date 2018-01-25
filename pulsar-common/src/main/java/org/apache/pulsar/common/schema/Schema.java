package org.apache.pulsar.common.schema;

import com.google.common.base.MoreObjects;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class Schema {
    public final SchemaType type;
    public final boolean isDeleted;
    public final String schemaInfo;
    public final long timestamp;
    public final String user;
    public final byte[] data;

    private Schema(Builder builder) {
        this.type = builder.type;
        this.isDeleted = builder.isDeleted;
        this.schemaInfo = builder.schemaInfo;
        this.timestamp = builder.timestamp;
        this.user = builder.user;
        this.data = builder.data;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("isDeleted", isDeleted)
            .add("schemaInfo", schemaInfo)
            .add("timestamp", timestamp)
            .add("user", user)
            .add("data", data)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        return isDeleted == schema.isDeleted &&
            timestamp == schema.timestamp &&
            type == schema.type &&
            Objects.equals(schemaInfo, schema.schemaInfo) &&
            Objects.equals(user, schema.user) &&
            Arrays.equals(data, schema.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, isDeleted, schemaInfo, timestamp, user);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    public static class Builder {
        private SchemaType type;
        private boolean isDeleted;
        private String schemaInfo = "";
        private long timestamp;
        private String user;
        private byte[] data;

        public Builder type(SchemaType type) {
            this.type = type;
            return this;
        }

        public Builder isDeleted(boolean isDeleted) {
            this.isDeleted = isDeleted;
            return this;
        }

        public Builder schemaInfo(String schemaInfo) {
            this.schemaInfo = schemaInfo;
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

        public Schema build() {
            checkNotNull(type);
            checkNotNull(schemaInfo);
            checkNotNull(user);
            checkNotNull(data);
            return new Schema(this);
        }

    }

    public static Builder newBuilder() {
        return new Builder();
    }
}