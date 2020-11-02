package org.apache.pulsar.common.functions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Configuration of extra pulsar clusters to sent output message.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ExternalPulsarConfig {
    private String name;
    private String serviceURL;
    private AuthenticationConfig authConfig;
    private ProducerConfig producerConfig;
}
