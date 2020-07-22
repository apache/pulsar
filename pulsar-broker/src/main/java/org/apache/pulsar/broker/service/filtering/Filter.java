package org.apache.pulsar.broker.service.filtering;

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;

import java.util.Properties;

public abstract class Filter {

    protected Properties properties;
    private Schema<GenericRecord> schema;

    Filter(Properties props) {
        properties = props;
    }

    public abstract boolean matches(ByteBuf val);

    public boolean isSchemaAware() {
        return false;
    }

    final public void setSchema(Schema<GenericRecord> schema) {
        this.schema = schema;
    }

    final public Schema<GenericRecord> getSchema() {
        return schema;
    }
}
