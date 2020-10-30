package org.apache.pulsar.common.functions;


import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;

/**
 * Configuration of the producer inside the function.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class CryptoConfig {
    private String cryptoKeyReaderClassName;
    private Map<String, Object> cryptoKeyReaderConfig;

    private String[] encryptionKeys;
    private ProducerCryptoFailureAction producerCryptoFailureAction;

    private ConsumerCryptoFailureAction consumerCryptoFailureAction;
}
