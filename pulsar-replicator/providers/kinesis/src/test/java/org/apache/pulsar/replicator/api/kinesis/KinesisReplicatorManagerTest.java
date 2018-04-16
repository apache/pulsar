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
package org.apache.pulsar.replicator.api.kinesis;

import java.util.Map;

import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.replicator.api.ReplicatorProducer;
import org.apache.pulsar.replicator.auth.DefaultAuthParamKeyStore;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Test for {@link KinesisReplicatorManager} 
 *
 */
public class KinesisReplicatorManagerTest {

    @Test
    public void testCreateProducer() throws Exception {
        final String topicName = "persistent://property/cluster/namespace/topic1";
        Map<String, String> authData = Maps.newHashMap();
        authData.put(KinesisReplicatorProvider.ACCESS_KEY_NAME, "a1");
        authData.put(KinesisReplicatorProvider.SECRET_KEY_NAME, "s1");
        ReplicatorPolicies policies = new ReplicatorPolicies();
        policies.topicNameMapping = Maps.newHashMap();
        policies.topicNameMapping.put("topic1", "stream-name:us-west-2");
        policies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();
        
        KinesisReplicatorManager manager = new KinesisReplicatorManager();
        ReplicatorProducer producer = manager.startProducer(topicName, policies).get();
        Assert.assertTrue(producer instanceof KinesisReplicatorProducer);
    }
}
