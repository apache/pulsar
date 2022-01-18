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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class PoliciesDataTest {

    @Test
    public void policies() {
        Policies policies = new Policies();

        assertEquals(policies, new Policies());

        policies.auth_policies.getNamespaceAuthentication().put("my-role", EnumSet.of(AuthAction.consume));

        assertNotEquals(new Policies(), policies);
        assertNotEquals(new Object(), policies);

        policies.auth_policies.getNamespaceAuthentication().clear();
        Map<String, Set<AuthAction>> permissions = new TreeMap<>();
        permissions.put("my-role", EnumSet.of(AuthAction.consume));
        policies.auth_policies.getTopicAuthentication().put("persistent://my-dest", permissions);

        assertNotEquals(new Policies(), policies);
    }

    @Test
    public void propertyAdmin() {
        TenantInfo pa1 = TenantInfo.builder()
                .adminRoles(Sets.newHashSet("role1", "role2"))
                .allowedClusters(Sets.newHashSet("use", "usw"))
                .build();

        assertEquals(pa1, TenantInfo.builder()
                .adminRoles(Sets.newHashSet("role1", "role2"))
                .allowedClusters(Sets.newHashSet("use", "usw"))
                .build());
        assertNotEquals(new Object(), pa1);
        assertNotEquals(TenantInfo.builder().build(), pa1);
        assertNotEquals(TenantInfo.builder().adminRoles(Sets.newHashSet("role1", "role3"))
                .allowedClusters(Sets.newHashSet("usc")).build(), pa1);
        assertEquals(pa1.getAdminRoles(), Lists.newArrayList("role1", "role2"));
    }

    @Test
    public void bundlesPolicies() throws JsonGenerationException, JsonMappingException, IOException {
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        String oldJsonPolicy = "{\"auth_policies\":{\"namespace_auth\":{},\"destination_auth\":{}},\"replication_clusters\":[],"
                + "\"backlog_quota_map\":{},\"persistence\":null,\"latency_stats_sample_rate\":{},\"message_ttl_in_seconds\":null}";
        Policies policies = jsonMapper.readValue(oldJsonPolicy.getBytes(), Policies.class);
        assertEquals(policies, new Policies());
        String newJsonPolicy = "{\"auth_policies\":{\"namespace_auth\":{},\"destination_auth\":{}},\"replication_clusters\":[],\"bundles\":null,"
                + "\"backlog_quota_map\":{},\"persistence\":null,\"latency_stats_sample_rate\":{},\"message_ttl_in_seconds\":null}";
        OldPolicies oldPolicies = jsonMapper.readValue(newJsonPolicy.getBytes(), OldPolicies.class);
        assertEquals(oldPolicies, new OldPolicies());
    }

    @Test
    public void bundlesData() throws IOException {
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        String newJsonPolicy = "{\"auth_policies\":{\"namespace_auth\":{},\"destination_auth\":{}},\"replication_clusters\":[],\"bundles\":{\"boundaries\":[\"0x00000000\",\"0xffffffff\"]},\"backlog_quota_map\":{},\"persistence\":null,\"latency_stats_sample_rate\":{}}";

        List<String> bundleSet = new ArrayList<>();
        bundleSet.add("0x00000000");
        bundleSet.add("0xffffffff");

        String newBundlesDataString = "{\"boundaries\":[\"0x00000000\",\"0xffffffff\"]}";
        BundlesData data = jsonMapper.readValue(newBundlesDataString.getBytes(), BundlesData.class);
        assertEquals(data.getBoundaries(), bundleSet);

        Policies policies = jsonMapper.readValue(newJsonPolicy.getBytes(), Policies.class);
        Policies expected = new Policies();
        expected.bundles = data;
        assertEquals(policies, expected);
    }
}
