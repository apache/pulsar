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
package org.apache.pulsar.common.policies.data.impl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.policies.data.DispatchRate;

/**
 * Dispatch rate.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class DispatchRateImpl implements DispatchRate {

    private int dispatchThrottlingRateInMsg;
    private long dispatchThrottlingRateInByte;
    private boolean relativeToPublishRate;
    private int ratePeriodInSecond;

    public static DispatchRateImplBuilder builder() {
        return new DispatchRateImplBuilder();
    }

    public static class DispatchRateImplBuilder implements DispatchRate.Builder {

        private int dispatchThrottlingRateInMsg = -1;
        private long dispatchThrottlingRateInByte = -1;
        private boolean relativeToPublishRate = false; /* throttles dispatch relatively publish-rate */
        private int ratePeriodInSecond = 1; /* by default dispatch-rate will be calculate per 1 second */


        public DispatchRateImplBuilder dispatchThrottlingRateInMsg(int dispatchThrottlingRateInMsg) {
            this.dispatchThrottlingRateInMsg = dispatchThrottlingRateInMsg;
            return this;
        }

        public DispatchRateImplBuilder dispatchThrottlingRateInByte(long dispatchThrottlingRateInByte) {
            this.dispatchThrottlingRateInByte = dispatchThrottlingRateInByte;
            return this;
        }

        public DispatchRateImplBuilder relativeToPublishRate(boolean relativeToPublishRate) {
            this.relativeToPublishRate = relativeToPublishRate;
            return this;
        }

        public DispatchRateImplBuilder ratePeriodInSecond(int ratePeriodInSecond) {
            this.ratePeriodInSecond = ratePeriodInSecond;
            return this;
        }

        public DispatchRateImpl build() {
            return new DispatchRateImpl(dispatchThrottlingRateInMsg, dispatchThrottlingRateInByte,
                    relativeToPublishRate, ratePeriodInSecond);
        }
    }
}
