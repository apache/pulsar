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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.util.HashMap;

import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.metadata.cache.impl.JSONMetadataSerdeSimpleType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BacklogQuotaCompatibilityTest {

    private final JavaType typeRef = TypeFactory.defaultInstance().constructSimpleType(Policies.class, null);

    private final JSONMetadataSerdeSimpleType<Policies> simpleType = new JSONMetadataSerdeSimpleType<>(typeRef);

    private final BacklogQuota.RetentionPolicy testPolicy = BacklogQuota.RetentionPolicy.consumer_backlog_eviction;

    @Test
    public void testV27ClientSetV28BrokerRead() throws Exception {
        Policies writePolicy = new Policies();
        BacklogQuotaImpl writeBacklogQuota = new BacklogQuotaImpl();
        writeBacklogQuota.setLimit(1024);
        writeBacklogQuota.setLimitTime(60);
        writeBacklogQuota.setPolicy(testPolicy);
        HashMap<BacklogQuota.BacklogQuotaType, BacklogQuota> quotaHashMap = new HashMap<>();
        quotaHashMap.put(BacklogQuota.BacklogQuotaType.destination_storage, writeBacklogQuota);
        writePolicy.backlog_quota_map = quotaHashMap;
        byte[] serialize = simpleType.serialize("/path", writePolicy);
        Policies policies = simpleType.deserialize("/path", serialize, null);
        BacklogQuota readBacklogQuota = policies.backlog_quota_map.get(BacklogQuota.BacklogQuotaType.destination_storage);
        Assert.assertEquals(readBacklogQuota.getLimitSize(), 1024);
        Assert.assertEquals(readBacklogQuota.getLimitTime(), 60);
        Assert.assertEquals(readBacklogQuota.getPolicy(), testPolicy);
    }

    @Test
    public void testV28ClientSetV28BrokerRead() throws Exception {
        Policies writePolicy = new Policies();
        BacklogQuotaImpl writeBacklogQuota = new BacklogQuotaImpl();
        writeBacklogQuota.setLimitSize(1024);
        writeBacklogQuota.setLimitTime(60);
        writeBacklogQuota.setPolicy(testPolicy);
        HashMap<BacklogQuota.BacklogQuotaType, BacklogQuota> quotaHashMap = new HashMap<>();
        quotaHashMap.put(BacklogQuota.BacklogQuotaType.destination_storage, writeBacklogQuota);
        writePolicy.backlog_quota_map = quotaHashMap;
        byte[] serialize = simpleType.serialize("/path", writePolicy);
        Policies policies = simpleType.deserialize("/path", serialize, null);
        BacklogQuota readBacklogQuota = policies.backlog_quota_map.get(BacklogQuota.BacklogQuotaType.destination_storage);
        Assert.assertEquals(readBacklogQuota.getLimit(), 1024);
        Assert.assertEquals(readBacklogQuota.getLimitTime(), 60);
        Assert.assertEquals(readBacklogQuota.getPolicy(), testPolicy);
    }

    @Test
    public void testV28ClientSetV27BrokerRead() {
        BacklogQuotaImpl writeBacklogQuota = new BacklogQuotaImpl();
        writeBacklogQuota.setLimitSize(1024);
        Assert.assertEquals(1024, writeBacklogQuota.getLimit());
    }

    @Test
    public void testBackwardCompatibility() throws IOException {
        String oldPolicyStr = "{\"auth_policies\":{\"namespace_auth\":{},\"destination_auth\":{},"
                + "\"subscription_auth_roles\":{}},\"replication_clusters\":[],\"backlog_quota_map\":"
                + "{\"destination_storage\":{\"limit\":1001,\"policy\":\"consumer_backlog_eviction\"}},"
                + "\"clusterDispatchRate\":{},\"topicDispatchRate\":{},\"subscriptionDispatchRate\":{},"
                + "\"replicatorDispatchRate\":{},\"clusterSubscribeRate\":{},\"publishMaxMessageRate\":{},"
                + "\"latency_stats_sample_rate\":{},\"subscription_expiration_time_minutes\":0,\"deleted\":false,"
                + "\"encryption_required\":false,\"subscription_auth_mode\":\"None\","
                + "\"max_consumers_per_subscription\":0,\"offload_threshold\":-1,"
                + "\"schema_auto_update_compatibility_strategy\":\"Full\",\"schema_compatibility_strategy\":"
                + "\"UNDEFINED\",\"is_allow_auto_update_schema\":true,\"schema_validation_enforced\":false,"
                + "\"subscription_types_enabled\":[]}\n";
        Policies policies = simpleType.deserialize(null, oldPolicyStr.getBytes(), null);
        assertEquals(policies.backlog_quota_map.get(BacklogQuota.BacklogQuotaType.destination_storage).getLimitSize(),
                1001);
        assertEquals(policies.backlog_quota_map.get(BacklogQuota.BacklogQuotaType.destination_storage).getLimitTime(),
                0);
        assertEquals(policies.backlog_quota_map.get(BacklogQuota.BacklogQuotaType.destination_storage).getPolicy(),
                BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
    }

    @Test
    public void testBackwardCompatibilityNullLimitAndLimitSize() throws IOException {
        String oldPolicyStr = "{\"auth_policies\":{\"namespace_auth\":{},\"destination_auth\":{},"
                + "\"subscription_auth_roles\":{}},\"replication_clusters\":[],\"backlog_quota_map\":"
                + "{\"destination_storage\":{\"policy\":\"consumer_backlog_eviction\"}},"
                + "\"clusterDispatchRate\":{},\"topicDispatchRate\":{},\"subscriptionDispatchRate\":{},"
                + "\"replicatorDispatchRate\":{},\"clusterSubscribeRate\":{},\"publishMaxMessageRate\":{},"
                + "\"latency_stats_sample_rate\":{},\"subscription_expiration_time_minutes\":0,\"deleted\":false,"
                + "\"encryption_required\":false,\"subscription_auth_mode\":\"None\","
                + "\"max_consumers_per_subscription\":0,\"offload_threshold\":-1,"
                + "\"schema_auto_update_compatibility_strategy\":\"Full\",\"schema_compatibility_strategy\":"
                + "\"UNDEFINED\",\"is_allow_auto_update_schema\":true,\"schema_validation_enforced\":false,"
                + "\"subscription_types_enabled\":[]}\n";
        Policies policies = simpleType.deserialize(null, oldPolicyStr.getBytes(), null);
        assertEquals(policies.backlog_quota_map.get(BacklogQuota.BacklogQuotaType.destination_storage).getLimitSize(),
                0);
        assertEquals(policies.backlog_quota_map.get(BacklogQuota.BacklogQuotaType.destination_storage).getLimitTime(),
                0);
    }

}
