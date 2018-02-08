package org.apache.pulsar.broker.service.schema;

final class LatestVersion implements SchemaVersion {
    @Override
    public long toLong() {
        return -1;
    }
}
