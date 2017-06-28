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

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Policies {

    public final AuthPolicies auth_policies;
    public List<String> replication_clusters;
    public BundlesData bundles;
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlog_quota_map;
    public PersistencePolicies persistence;
    public Map<String, Integer> latency_stats_sample_rate;
    public int message_ttl_in_seconds;
    public RetentionPolicies retention_policies;
    public boolean deleted;

    public static final String FIRST_BOUNDARY = "0x00000000";
    public static final String LAST_BOUNDARY = "0xffffffff";

    public Policies() {
        auth_policies = new AuthPolicies();
        replication_clusters = Lists.newArrayList();
        bundles = defaultBundle();
        backlog_quota_map = Maps.newHashMap();
        persistence = null;
        latency_stats_sample_rate = Maps.newHashMap();
        message_ttl_in_seconds = 0;
        retention_policies = null;
        deleted = false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Policies) {
            Policies other = (Policies) obj;
            return Objects.equal(auth_policies, other.auth_policies)
                    && Objects.equal(replication_clusters, other.replication_clusters)
                    && Objects.equal(backlog_quota_map, other.backlog_quota_map)
                    && Objects.equal(persistence, other.persistence) && Objects.equal(bundles, other.bundles)
                    && Objects.equal(latency_stats_sample_rate, other.latency_stats_sample_rate)
                    && message_ttl_in_seconds == other.message_ttl_in_seconds
                    && Objects.equal(retention_policies, other.retention_policies);
        }

        return false;
    }

    public static BundlesData defaultBundle() {
        BundlesData bundle = new BundlesData(1);
        List<String> boundaries = Lists.newArrayList();
        boundaries.add(FIRST_BOUNDARY);
        boundaries.add(LAST_BOUNDARY);
        bundle.setBoundaries(boundaries);
        return bundle;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("auth_policies", auth_policies)
                .add("replication_clusters", replication_clusters).add("bundles", bundles)
                .add("backlog_quota_map", backlog_quota_map).add("persistence", persistence)
                .add("latency_stats_sample_rate", latency_stats_sample_rate)
                .add("message_ttl_in_seconds", message_ttl_in_seconds).add("retention_policies", retention_policies)
                .add("deleted", deleted).toString();
    }
    
}
