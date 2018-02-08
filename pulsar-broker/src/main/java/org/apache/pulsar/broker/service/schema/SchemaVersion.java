package org.apache.pulsar.broker.service.schema;

public interface SchemaVersion {

    SchemaVersion Latest = new LatestVersion();

    long toLong();

    static SchemaVersion fromLong(long version) {
        return new LongSchemaVersion(version);
    }

}
