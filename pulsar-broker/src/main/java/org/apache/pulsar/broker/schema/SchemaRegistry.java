package org.apache.pulsar.broker.schema;

public interface SchemaRegistry {

    Schema getSchema(String schemaId);

    Schema getSchema(String schemaId, int version);

    int putSchema(String schemaId, SchemaType type, String schema);

}
