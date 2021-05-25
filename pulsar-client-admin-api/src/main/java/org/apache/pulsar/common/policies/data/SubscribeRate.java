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
import lombok.ToString;

/**
 * Information about subscription rate.
 */
@ToString
public class SubscribeRate {

    public int subscribeThrottlingRatePerConsumer = -1;
    public int ratePeriodInSecond = 30;

    public SubscribeRate() {
    }

    public SubscribeRate(int subscribeThrottlingRatePerConsumer, int ratePeriodInSecond) {
        this.subscribeThrottlingRatePerConsumer = subscribeThrottlingRatePerConsumer;
        this.ratePeriodInSecond = ratePeriodInSecond;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscribeThrottlingRatePerConsumer, ratePeriodInSecond);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SubscribeRate) {
            SubscribeRate rate = (SubscribeRate) obj;
            return Objects.equals(subscribeThrottlingRatePerConsumer, rate.subscribeThrottlingRatePerConsumer)
                    && Objects.equals(ratePeriodInSecond, rate.ratePeriodInSecond);
        }
        return false;
    }

}
