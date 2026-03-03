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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OldPolicies {
    public final AuthPolicies authPolicies;
    public List<String> replicationClusters;
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlogQuotaMap;
    public PersistencePolicies persistence;
    public Map<String, Integer> latencyStatsSampleRate;

    public OldPolicies() {
        authPolicies = AuthPolicies.builder().build();
        replicationClusters = new ArrayList<>();
        backlogQuotaMap = new HashMap<>();
        persistence = null;
        latencyStatsSampleRate = new HashMap<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof OldPolicies) {
            OldPolicies other = (OldPolicies) obj;
            return Objects.equals(authPolicies, other.authPolicies)
                    && Objects.equals(replicationClusters, other.replicationClusters)
                    && Objects.equals(backlogQuotaMap, other.backlogQuotaMap)
                    && Objects.equals(persistence, other.persistence)
                    && Objects.equals(latencyStatsSampleRate, other.latencyStatsSampleRate);
        }

        return false;
    }

}
