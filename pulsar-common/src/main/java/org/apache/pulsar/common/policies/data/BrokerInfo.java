package org.apache.pulsar.common.policies.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Broker Information
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BrokerInfo {
    private String serviceUrl;
}
