package org.apache.pulsar.broker.schema;

import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;

public interface SchemaRegistry {

    Schema getSchema(String schemaId);

    Schema getSchema(String schemaId, int version);

    int putSchema(String schemaId, SchemaType type, String schema);

}
