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
import org.apache.pulsar.common.policies.data.BrokerInfo;

/**
 * Broker Information.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class BrokerInfoImpl implements BrokerInfo {
    private String serviceUrl;

    public static BrokerInfoImplBuilder builder() {
        return new BrokerInfoImplBuilder();
    }

    public static class BrokerInfoImplBuilder implements BrokerInfo.Builder {
        private String serviceUrl;

        public BrokerInfoImplBuilder serviceUrl(String serviceUrl) {
            this.serviceUrl = serviceUrl;
            return this;
        }

        public BrokerInfoImpl build() {
            return new BrokerInfoImpl(serviceUrl);
        }
    }
}
