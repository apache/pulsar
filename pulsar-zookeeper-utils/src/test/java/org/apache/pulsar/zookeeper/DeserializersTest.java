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
package org.apache.pulsar.zookeeper;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.common.policies.data.Policies;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

public class DeserializersTest {

    @BeforeMethod
    void setup() throws Exception {
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
    }

    @Test
    public void testSimpleStringDeserialize() throws Exception {
        String key = "test_key";
        byte[] content = "test_content".getBytes(StandardCharsets.UTF_8);
        String result = Deserializers.STRING_DESERIALIZER.deserialize(key, content);
        assertEquals(result, "test_content");
    }

    @Test
    public void testSimplePolicyDeserialize() throws Exception {
        String key = "test_key";
        String jsonPolicy = "{\"auth_policies\":{\"namespace_auth\":{},\"destination_auth\":{}},\"replication_clusters\":[],"
                + "\"bundles_activated\":true,\"backlog_quota_map\":{},\"persistence\":null,\"latency_stats_sample_rate\":{},\"message_ttl_in_seconds\":null}";
        byte[] content = jsonPolicy.getBytes(StandardCharsets.UTF_8);
        Policies result = Deserializers.POLICY_DESERIALIZER.deserialize(key, content);
        assertEquals(result, new Policies());
    }
}
