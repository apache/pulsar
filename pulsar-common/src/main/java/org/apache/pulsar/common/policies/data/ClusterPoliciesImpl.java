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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The configuration data for a cluster.
 */
@ApiModel(
        value = "ClusterPolicies",
        description = "The local cluster policies for a cluster"
)
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class ClusterPoliciesImpl implements  ClusterPolicies, Cloneable {
    @ApiModelProperty(
            name = "migrated",
            value = "flag to check if cluster is migrated to different cluster",
            example = "true/false"
    )
    private boolean migrated;
    @ApiModelProperty(
            name = "migratedClusterUrl",
            value = "url of cluster where current cluster is migrated"
    )
    private ClusterUrl migratedClusterUrl;

    public static ClusterPoliciesImplBuilder builder() {
        return new ClusterPoliciesImplBuilder();
    }

    @Override
    public ClusterPoliciesImplBuilder clone() {
        return builder()
                .migrated(migrated)
                .migratedClusterUrl(migratedClusterUrl);
    }

    @Data
    public static class ClusterPoliciesImplBuilder implements ClusterPolicies.Builder {
        private boolean migrated;
        private ClusterUrl migratedClusterUrl;

        ClusterPoliciesImplBuilder() {
        }

        public ClusterPoliciesImplBuilder migrated(boolean migrated) {
            this.migrated = migrated;
            return this;
        }

        public ClusterPoliciesImplBuilder migratedClusterUrl(ClusterUrl migratedClusterUrl) {
            this.migratedClusterUrl = migratedClusterUrl;
            return this;
        }

        public ClusterPoliciesImpl build() {
            return new ClusterPoliciesImpl(
                    migrated,
                    migratedClusterUrl);
        }
    }
}
