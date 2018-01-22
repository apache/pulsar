package org.apache.pulsar.common.schema;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Schema {
    public final SchemaType type;
    public final long version;
    public final boolean isDeleted;
    public final String schemaInfo;
    public final String id;
    public final long timestamp;
    public final String user;
    public final byte[] data;

    private Schema(Builder builder) {
        this.type = builder.type;
        this.version = builder.version;
        this.isDeleted = builder.isDeleted;
        this.schemaInfo = builder.schemaInfo;
        this.id = builder.id;
        this.timestamp = builder.timestamp;
        this.user = builder.user;
        this.data = builder.data;
    }

    public static class Builder {
        private SchemaType type;
        private long version;
        private boolean isDeleted;
        private String schemaInfo;
        private String id;
        private long timestamp;
        private String user;
        private byte[] data;

        public Builder type(SchemaType type) {
            this.type = type;
            return this;
        }

        public Builder version(long version) {
            this.version = version;
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

        public Builder id(String id) {
            this.id = id;
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
            checkArgument(version > 0);
            checkNotNull(schemaInfo);
            checkNotNull(id);
            checkArgument(timestamp > 0);
            checkNotNull(user);
            checkNotNull(data);
            return new Schema(this);
        }

    }

    public static Builder newBuilder() {
        return new Builder();
    }
}