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

import org.apache.pulsar.client.api.RedeliveryBackoff;

public class LogIndexBackoff implements RedeliveryBackoff {

    private final long minLag;
    private final long maxLag;
    private final double exponent;

    public LogIndexBackoff(long minLag, long maxLag, double exponent) {
        this.minLag = minLag;
        this.maxLag = maxLag;
        this.exponent = exponent;
    }

    public long getMinLag() {
        return this.minLag;
    }

    public long getMaxLag() {
        return this.maxLag;
    }

    public double getExponent() {
        return exponent;
    }

    @Override
    public long next(int indexCount) {
        if (indexCount <= 0 || minLag <= 0) {
            return 0;
        }
        if (maxLag != -1) {
            return (long) Math.min(this.maxLag, minLag * Math.pow(indexCount, exponent));
        } else {
            return (long) (minLag * Math.pow(indexCount, exponent));
        }
    }
}
