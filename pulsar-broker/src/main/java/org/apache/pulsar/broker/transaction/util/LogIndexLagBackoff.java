/**
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
package org.apache.pulsar.broker.transaction.util;

import static com.google.common.base.Preconditions.checkArgument;
import lombok.Getter;

public class LogIndexLagBackoff {

    @Getter
    private final long minLag;
    @Getter
    private final long maxLag;
    @Getter
    private final double exponent;

    public LogIndexLagBackoff(long minLag, long maxLag, double exponent) {
        checkArgument(minLag > 0, "min lag must be > 0");
        checkArgument(maxLag >= minLag, "maxLag should be >= minLag");
        checkArgument(exponent > 0, "exponent must be > 0");
        this.minLag = minLag;
        this.maxLag = maxLag;
        this.exponent = exponent;
    }


    public long next(int indexCount) {
        if (indexCount <= 0) {
            return minLag;
        }
        return (long) Math.min(this.maxLag, minLag * Math.pow(indexCount, exponent));
    }
}
