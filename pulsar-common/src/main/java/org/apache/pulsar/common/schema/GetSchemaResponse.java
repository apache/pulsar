package org.apache.pulsar.common.schema;

import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GetSchemaResponse {
    private SchemaVersion version;
    private SchemaType type;
    private long timestamp;
    private String data;
    private Map<String, String> props;
}
