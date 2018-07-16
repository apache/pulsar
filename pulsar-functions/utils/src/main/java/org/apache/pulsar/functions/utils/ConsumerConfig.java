package org.apache.pulsar.functions.utils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerConfig {
    private String schemaTypeOrClassName;
    private boolean isRegexPattern;
}
