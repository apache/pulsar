package org.apache.pulsar.common.schema;

import java.util.Map;
import lombok.Data;

@Data
public class SchemaInfo {
    private String name;
    private byte[] schema;
    private SchemaType type;
    private Map<String, String> properties;
}
