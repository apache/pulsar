package org.apache.pulsar.common.protocol.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GetAllVersionsSchemaResponse {
    private List<GetSchemaResponse> getSchemaResponses;
}
