package org.apache.pulsar.broker.schema;

public interface Schema {

    SchemaType getType();

    int getVersion();

    boolean isDeleted();

    String getSchemaInfo();

}