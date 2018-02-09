package org.apache.pulsar.common.schema;

public interface SchemaVersion {
    SchemaVersion Latest = new LatestVersion();

    byte[] bytes();

}
