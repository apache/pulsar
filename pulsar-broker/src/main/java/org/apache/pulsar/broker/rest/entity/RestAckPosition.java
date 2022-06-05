package org.apache.pulsar.broker.rest.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RestAckPosition {
    long ledgerId;
    long entryId;
    int partition;
}
