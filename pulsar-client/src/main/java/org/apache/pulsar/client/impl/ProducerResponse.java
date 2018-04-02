package org.apache.pulsar.client.impl;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ProducerResponse {
    private String producerName;
    private long lastSequenceId;
    private byte[] schemaVersion;
}
