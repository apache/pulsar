package org.apache.pulsar.common.schema;

import java.util.Map;
import lombok.Data;

@Data
public class PostSchemaPayload {
    private String type;
    private String schema;
    private Map<String, String> properties;
}
