package org.apache.pulsar.common.schema;

final class LatestVersion implements SchemaVersion {
    private static final byte[] EMPTY = new byte[]{};

    @Override
    public byte[] bytes() {
        return EMPTY;
    }
}
