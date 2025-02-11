/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.functions;

import static org.apache.pulsar.common.policies.data.impl.BookieInfoImpl.BookieInfoImplBuilder.checkArgument;
import java.util.Optional;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
/**
 * Memory limit set for the pulsar client used by all instances
 * If `absoluteValue` and `percentOfMaxDirectMemory` are both set, then the min of the two will be used.
 */
public class MemoryLimit {
    Long absoluteValue;
    Double percentOfMaxDirectMemory;
    public static Optional<Long> calculateClientMemoryLimit(Optional<MemoryLimit> memoryLimit) {
        if (memoryLimit.isPresent()) {

            Long absolute = memoryLimit.get().getAbsoluteValue();
            Double percentOfDirectMem = memoryLimit.get().getPercentOfMaxDirectMemory();
            if (absolute != null) {
                checkArgument(absolute > 0, "Absolute memory limit for Pulsar client has to be positive");
            }
            if (percentOfDirectMem != null) {
                checkArgument(percentOfDirectMem > 0 && percentOfDirectMem <= 100,
                        "Percent of max direct memory limit for Pulsar client must be between 0 and 100");
            }

            if (absolute != null && percentOfDirectMem != null) {
                return Optional.of(Math.min(absolute, getBytesPercentDirectMem(percentOfDirectMem)));
            }

            if (absolute != null) {
                return Optional.of(absolute);
            }

            if (percentOfDirectMem != null) {
                return Optional.of(getBytesPercentDirectMem(percentOfDirectMem));
            }
        }
        return Optional.empty();
    }

    private static long getBytesPercentDirectMem(double percent) {
        return (long) (DirectMemoryUtils.jvmMaxDirectMemory() * (percent / 100));
    }
}