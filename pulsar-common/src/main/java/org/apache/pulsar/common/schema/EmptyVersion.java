package org.apache.pulsar.common.schema;

final public class EmptyVersion implements SchemaVersion {
    private static final byte[] EMPTY = new byte[]{};

    @Override
    public byte[] bytes() {
        return EMPTY;
    }
}
