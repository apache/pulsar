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
 * Publish-rate to manage publish throttling.
 */
@ToString
public class PublishRate {

    public int publishThrottlingRateInMsg = -1;
    public long publishThrottlingRateInByte = -1;

    public PublishRate() {
        super();
        this.publishThrottlingRateInMsg = -1;
        this.publishThrottlingRateInByte = -1;
    }

    public PublishRate(int dispatchThrottlingRateInMsg, long dispatchThrottlingRateInByte) {
        super();
        this.publishThrottlingRateInMsg = dispatchThrottlingRateInMsg;
        this.publishThrottlingRateInByte = dispatchThrottlingRateInByte;
    }

    public static PublishRate normalize(PublishRate publishRate) {
        if (publishRate != null
            && (publishRate.publishThrottlingRateInMsg > 0
            || publishRate.publishThrottlingRateInByte > 0)) {
            return publishRate;
        } else {
            return null;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(publishThrottlingRateInMsg, publishThrottlingRateInByte);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PublishRate) {
            PublishRate rate = (PublishRate) obj;
            return Objects.equals(publishThrottlingRateInMsg, rate.publishThrottlingRateInMsg)
                    && Objects.equals(publishThrottlingRateInByte, rate.publishThrottlingRateInByte);
        }
        return false;
    }

}
