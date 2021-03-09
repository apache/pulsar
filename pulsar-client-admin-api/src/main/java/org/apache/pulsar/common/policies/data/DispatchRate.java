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
package org.apache.pulsar.common.policies.data;

import com.google.common.base.MoreObjects;
import java.util.Objects;

/**
 * Dispatch rate.
 */
public class DispatchRate {

    public int dispatchThrottlingRateInMsg = -1;
    public long dispatchThrottlingRateInByte = -1;
    public boolean relativeToPublishRate = false; /* throttles dispatch relatively publish-rate */
    public int ratePeriodInSecond = 1; /* by default dispatch-rate will be calculate per 1 second */

    public DispatchRate() {
        super();
        this.dispatchThrottlingRateInMsg = -1;
        this.dispatchThrottlingRateInByte = -1;
        this.ratePeriodInSecond = 1;
    }

    public DispatchRate(int dispatchThrottlingRateInMsg, long dispatchThrottlingRateInByte,
            int ratePeriodInSecond) {
        super();
        this.dispatchThrottlingRateInMsg = dispatchThrottlingRateInMsg;
        this.dispatchThrottlingRateInByte = dispatchThrottlingRateInByte;
        this.ratePeriodInSecond = ratePeriodInSecond;
    }

    public DispatchRate(int dispatchThrottlingRateInMsg, long dispatchThrottlingRateInByte,
            int ratePeriodInSecond, boolean relativeToPublishRate) {
        this(dispatchThrottlingRateInMsg, dispatchThrottlingRateInByte, ratePeriodInSecond);
        this.relativeToPublishRate = relativeToPublishRate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dispatchThrottlingRateInMsg, dispatchThrottlingRateInByte,
                ratePeriodInSecond);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DispatchRate) {
            DispatchRate rate = (DispatchRate) obj;
            return Objects.equals(dispatchThrottlingRateInMsg, rate.dispatchThrottlingRateInMsg)
                    && Objects.equals(dispatchThrottlingRateInByte, rate.dispatchThrottlingRateInByte)
                    && Objects.equals(ratePeriodInSecond, rate.ratePeriodInSecond);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("dispatchThrottlingRateInMsg", dispatchThrottlingRateInMsg)
                .add("dispatchThrottlingRateInByte", dispatchThrottlingRateInByte)
                .add("ratePeriodInSecond", ratePeriodInSecond).toString();
    }

}
