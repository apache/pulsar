package org.apache.pulsar.broker.service.filtering;

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.client.api.schema.GenericRecord;

import java.util.Map;
import java.util.Properties;

public class BasicAvroFilter extends Filter {

    public BasicAvroFilter(Properties props) {
        super(props);
    }

    @Override
    public boolean matches(ByteBuf val) {
        byte[] b = new byte[val.readableBytes()];
        val.readBytes(b);
        GenericRecord gr = getSchema().decode(b);

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (!gr.getField((String) entry.getKey()).equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isSchemaAware() {
        return true;
    }
}
