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
package org.apache.pulsar.broker.admin.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.policies.data.AppliedTopicPolicies;
import org.apache.pulsar.common.policies.data.AppliedValue;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicySource;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

public class TopicPolicyResolverTest {

    private static TopicPolicyResolver resolver() {
        return new TopicPolicyResolver(new ServiceConfiguration(), BacklogQuotaImpl.builder().build());
    }

    @Test
    public void testEveryPolicyIsResolved() throws Exception {
        AppliedTopicPolicies applied = resolver().resolve(new TopicPolicies(), new TopicPolicies(), new Policies());
        JsonNode json = ObjectMapperFactory.getMapper().reader().readTree(
                ObjectMapperFactory.getMapper().writer().writeValueAsString(applied));
        Set<String> missing = new TreeSet<>();
        for (Field field : AppliedTopicPolicies.class.getDeclaredFields()) {
            if (field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            // Policies that exist only at topic level are absent when unset.
            if (!json.has(field.getName()) && !Set.of("shadowTopics", "subscriptionPolicies")
                    .contains(field.getName())) {
                missing.add(field.getName());
            }
        }
        // A policy added to TopicPolicies and AppliedTopicPolicies, but not resolved, would be dropped from the
        // response because null fields are not serialized.
        assertTrue(missing.isEmpty(), "not resolved by TopicPolicyResolver: " + missing);
    }

    @Test
    public void testLocalThenGlobalThenNamespace() {
        Policies ns = new Policies();
        ns.retention_policies = new RetentionPolicies(10, 10);
        TopicPolicies local = new TopicPolicies();
        TopicPolicies global = new TopicPolicies();

        AppliedValue<RetentionPolicies> value = resolver().resolve(local, global, ns).getRetentionPolicies();
        assertEquals(value.getSource(), PolicySource.NAMESPACE);

        global.setRetentionPolicies(new RetentionPolicies(60, 60));
        value = resolver().resolve(local, global, ns).getRetentionPolicies();
        assertEquals(value.getSource(), PolicySource.TOPIC_GLOBAL);
        assertEquals(value.getValue(), new RetentionPolicies(60, 60));

        local.setRetentionPolicies(new RetentionPolicies(30, 30));
        value = resolver().resolve(local, global, ns).getRetentionPolicies();
        assertEquals(value.getSource(), PolicySource.TOPIC);
        assertEquals(value.getValue(), new RetentionPolicies(30, 30));
    }

    @Test
    public void testBacklogQuotaLocalThenGlobal() {
        TopicPolicies local = new TopicPolicies();
        TopicPolicies global = new TopicPolicies();
        BacklogQuotaImpl quota = BacklogQuotaImpl.builder().limitSize(100).build();
        global.getBackLogQuotaMap().put(BacklogQuota.BacklogQuotaType.destination_storage.name(), quota);
        AppliedValue<BacklogQuotaImpl> value = resolver().resolve(local, global, new Policies()).getBackLogQuotaMap()
                .get(BacklogQuota.BacklogQuotaType.destination_storage.name());
        assertEquals(value.getSource(), PolicySource.TOPIC_GLOBAL);
        assertEquals(value.getValue(), quota);
    }

    @Test
    public void testAutoSubscriptionCreationIgnoresGlobalPolicies() {
        TopicPolicies global = new TopicPolicies();
        global.setAutoSubscriptionCreationOverride(
                org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl.builder()
                        .allowAutoSubscriptionCreation(false).build());
        AppliedValue<?> value = resolver().resolve(new TopicPolicies(), global, new Policies())
                .getAutoSubscriptionCreationOverride();
        assertEquals(value.getSource(), PolicySource.BROKER);
    }
}
