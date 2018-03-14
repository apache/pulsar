package org.apache.pulsar.common.schema;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PostSchemaResponse {
    private SchemaVersion version;
}
