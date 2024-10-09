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
package org.apache.pulsar.common.policies.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.admin.utils.ReflectionUtils;

public interface ClusterPolicies {
    boolean isMigrated();

    ClusterUrl getMigratedClusterUrl();

    interface Builder {
        Builder migrated(boolean migrated);

        Builder migratedClusterUrl(ClusterUrl migratedClusterUrl);

        ClusterPolicies build();
    }

    Builder clone();

    static Builder builder() {
        return ReflectionUtils.newBuilder("org.apache.pulsar.common.policies.data.ClusterPoliciesImpl");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    class ClusterUrl {
        String serviceUrl;
        String serviceUrlTls;
        String brokerServiceUrl;
        String brokerServiceUrlTls;

        public boolean isEmpty() {
            return serviceUrl != null && serviceUrlTls != null && brokerServiceUrl == null
                    && brokerServiceUrlTls == null;
        }
    }
}
