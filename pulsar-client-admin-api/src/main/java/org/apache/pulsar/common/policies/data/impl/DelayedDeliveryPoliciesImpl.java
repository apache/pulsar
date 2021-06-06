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
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;

/**
 * Definition of the delayed delivery policy.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class DelayedDeliveryPoliciesImpl implements DelayedDeliveryPolicies {
    private long tickTime;
    private boolean active;

    public static DelayedDeliveryPoliciesImplBuilder builder() {
        return new DelayedDeliveryPoliciesImplBuilder();
    }

    public static class DelayedDeliveryPoliciesImplBuilder implements DelayedDeliveryPolicies.Builder {
        private long tickTime;
        private boolean active;

        public DelayedDeliveryPoliciesImplBuilder tickTime(long tickTime) {
            this.tickTime = tickTime;
            return this;
        }

        public DelayedDeliveryPoliciesImplBuilder active(boolean active) {
            this.active = active;
            return this;
        }

        public DelayedDeliveryPoliciesImpl build() {
            return new DelayedDeliveryPoliciesImpl(tickTime, active);
        }
    }
}
