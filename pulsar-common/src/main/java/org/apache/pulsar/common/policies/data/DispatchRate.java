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

import java.util.Objects;

import com.google.common.base.MoreObjects;

public class DispatchRate {

    public int dispatchThrottlingRatePerTopicInMsg = -1;
    public long dispatchThrottlingRatePerTopicInByte = -1;
    public int ratePeriodInSecond = 1; /* by default dispatch-rate will be calculate per 1 second */

    public DispatchRate() {
        super();
        this.dispatchThrottlingRatePerTopicInMsg = -1;
        this.dispatchThrottlingRatePerTopicInByte = -1;
        this.ratePeriodInSecond = 1;
    }

    public DispatchRate(int dispatchThrottlingRatePerTopicInMsg, long dispatchThrottlingRatePerTopicInByte,
            int ratePeriodInSecond) {
        super();
        this.dispatchThrottlingRatePerTopicInMsg = dispatchThrottlingRatePerTopicInMsg;
        this.dispatchThrottlingRatePerTopicInByte = dispatchThrottlingRatePerTopicInByte;
        this.ratePeriodInSecond = ratePeriodInSecond;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dispatchThrottlingRatePerTopicInMsg, dispatchThrottlingRatePerTopicInByte,
                ratePeriodInSecond);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DispatchRate) {
            DispatchRate rate = (DispatchRate) obj;
            return Objects.equals(dispatchThrottlingRatePerTopicInMsg, rate.dispatchThrottlingRatePerTopicInMsg)
                    && Objects.equals(dispatchThrottlingRatePerTopicInByte, rate.dispatchThrottlingRatePerTopicInByte)
                    && Objects.equals(ratePeriodInSecond, rate.ratePeriodInSecond);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("dispatchThrottlingRatePerTopicInMsg", dispatchThrottlingRatePerTopicInMsg)
                .add("dispatchThrottlingRatePerTopicInByte", dispatchThrottlingRatePerTopicInByte)
                .add("ratePeriodInSecond", ratePeriodInSecond).toString();
    }

}
