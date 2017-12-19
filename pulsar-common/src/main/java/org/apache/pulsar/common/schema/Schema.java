package org.apache.pulsar.common.schema;

public interface Schema {

    SchemaType getType();

    int getVersion();

    boolean isDeleted();

    String getSchemaInfo();

}