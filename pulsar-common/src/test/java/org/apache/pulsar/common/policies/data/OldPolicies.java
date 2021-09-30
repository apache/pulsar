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

import java.util.List;
import java.util.Map;

import org.apache.pulsar.common.policies.data.AuthPolicies;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.PersistencePolicies;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class OldPolicies {
    public final AuthPolicies auth_policies;
    public List<String> replication_clusters;
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlog_quota_map;
    public PersistencePolicies persistence;
    public Map<String, Integer> latency_stats_sample_rate;

    public OldPolicies() {
        auth_policies = AuthPolicies.builder().build();
        replication_clusters = Lists.newArrayList();
        backlog_quota_map = Maps.newHashMap();
        persistence = null;
        latency_stats_sample_rate = Maps.newHashMap();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof OldPolicies) {
            OldPolicies other = (OldPolicies) obj;
            return Objects.equal(auth_policies, other.auth_policies)
                    && Objects.equal(replication_clusters, other.replication_clusters)
                    && Objects.equal(backlog_quota_map, other.backlog_quota_map)
                    && Objects.equal(persistence, other.persistence)
                    && Objects.equal(latency_stats_sample_rate, other.latency_stats_sample_rate);
        }

        return false;
    }

}
