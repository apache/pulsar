package org.apache.pulsar.broker.service.schema;

import com.google.common.base.MoreObjects;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.pulsar.common.schema.SchemaVersion;

public class StoredSchema {
    public final byte[] data;
    public final SchemaVersion version;
    public final Map<String, String> metadata;

    public StoredSchema(byte[] data, SchemaVersion version, Map<String, String> metadata) {
        this.data = data;
        this.version = version;
        this.metadata = metadata;
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
        return Arrays.equals(data, that.data) &&
            Objects.equals(version, that.version) &&
            Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(version, metadata);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("data", data)
            .add("version", version)
            .add("metadata", metadata)
            .toString();
    }
}
