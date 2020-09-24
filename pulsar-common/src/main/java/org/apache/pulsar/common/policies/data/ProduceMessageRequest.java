package org.apache.pulsar.common.policies.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@AllArgsConstructor
@Setter
@Getter
@NoArgsConstructor
public class ProduceMessageRequest {

    private int schemaVersion;

    private String keySchema;

    private String valueSchema;

    private String messages;

}
