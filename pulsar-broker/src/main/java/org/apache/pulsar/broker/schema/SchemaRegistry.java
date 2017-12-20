package org.apache.pulsar.broker.schema;

import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;

public interface SchemaRegistry extends AutoCloseable {

    Schema getSchema(String schemaId);

    Schema getSchema(String schemaId, long version);

    long putSchema(String schemaId, SchemaType type, String schema);

}
