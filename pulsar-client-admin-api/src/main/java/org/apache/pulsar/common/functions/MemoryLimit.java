package org.apache.pulsar.common.functions;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
/**
 * Memory limit set for the pulsar client used by all instances
 * If `absoluteValue` and `percentOfMaxDirectMemory` are both set, then the min of the two will be used.
 */
public  class MemoryLimit {
    Long absoluteValue;
    Double percentOfMaxDirectMemory;
}