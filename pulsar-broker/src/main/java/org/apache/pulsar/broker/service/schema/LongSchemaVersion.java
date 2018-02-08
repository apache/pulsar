package org.apache.pulsar.broker.service.schema;

import com.google.common.base.MoreObjects;
import java.util.Objects;

class LongSchemaVersion implements SchemaVersion {
    private final long version;

    LongSchemaVersion(long version) {
        this.version = version;
    }

    @Override
    public long toLong() {
        return version;
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
