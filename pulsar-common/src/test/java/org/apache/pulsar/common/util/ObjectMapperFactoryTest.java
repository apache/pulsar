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
package org.apache.pulsar.common.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.pulsar.common.policies.data.CompactionStats;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.CompactionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.stats.Metrics;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ObjectMapperFactoryTest {

    @Test
    public void testResourceQuotaMixIn() {
        ObjectMapper objectMapper = ObjectMapperFactory.getMapper().getObjectMapper();
        try {
            ResourceQuota resourceQuota = new ResourceQuota();
            String json = objectMapper.writeValueAsString(resourceQuota);
            Assert.assertFalse(json.contains("valid"));
        } catch (Exception ex) {
            Assert.fail("shouldn't have thrown exception", ex);
        }
    }

    @Test
    public void testMetricsMixIn() {
        ObjectMapper objectMapper = ObjectMapperFactory.getMapper().getObjectMapper();
        try {
            Metrics metrics = new Metrics();
            String json = objectMapper.writeValueAsString(metrics);
            Assert.assertTrue(json.contains("dimensions"));
        } catch (Exception ex) {
            Assert.fail("shouldn't have thrown exception", ex);
        }
    }

    @Test
    public void testAbstractTypeMappingsDeserializeToImplementation() throws Exception {
        ObjectMapper objectMapper = ObjectMapperFactory.getMapper().getObjectMapper();
        assertThat(ObjectMapperFactory.getAbstractTypeMappings())
                .containsEntry(TopicStats.class, TopicStatsImpl.class)
                .containsEntry(CompactionStats.class, CompactionStatsImpl.class)
                .hasSize(36);
        for (Map.Entry<Class<?>, Class<?>> mapping : ObjectMapperFactory.getAbstractTypeMappings().entrySet()) {
            assertThat(objectMapper.readValue("{}", mapping.getKey()))
                    .as("value deserialized as %s", mapping.getKey().getSimpleName())
                    .isExactlyInstanceOf(mapping.getValue());
        }
    }

    @Test
    public void testAbstractTypeMappingsAreUnmodifiable() {
        assertThatThrownBy(() -> ObjectMapperFactory.getAbstractTypeMappings().put(Object.class, Object.class))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
