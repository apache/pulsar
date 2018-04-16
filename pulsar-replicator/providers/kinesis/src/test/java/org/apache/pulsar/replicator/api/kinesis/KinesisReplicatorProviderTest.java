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

import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.replicator.api.ReplicatorProducer;
import org.apache.pulsar.replicator.auth.DefaultAuthParamKeyStore;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Test for {@link KinesisReplicatorProvider}
 */
public class KinesisReplicatorProviderTest {

    @Test
    public void testGetType() throws Exception {
        KinesisReplicatorProvider provider = KinesisReplicatorProvider.instance();
        Assert.assertEquals(provider.getType(), ReplicatorType.Kinesis);
    }

    @Test
    public void testValidatePropertiesAuthData() throws Exception {
        KinesisReplicatorProvider provider = KinesisReplicatorProvider.instance();
        try {
            provider.validateProperties(null, null);
            Assert.fail("auth data can't be null");
        } catch (IllegalArgumentException ie) {// expected
        }

        try {
            provider.validateProperties(null, null);
            Assert.fail("auth data must have access and secret key");
        } catch (IllegalArgumentException ie) {// expected
        }

        Map<String, String> properties = Maps.newHashMap();
        properties.put(KinesisReplicatorProvider.ACCESS_KEY_NAME, "a1");
        properties.put(KinesisReplicatorProvider.SECRET_KEY_NAME, "s1");
        ReplicatorPolicies policies = new ReplicatorPolicies();
        policies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();
        policies.replicationProperties = properties;
        provider.validateProperties(null, policies);
    }

    @Test
    public void testValidatePropertiesTopicMapping() throws Exception {
        KinesisReplicatorProvider provider = KinesisReplicatorProvider.instance();

        Map<String, String> properties = Maps.newHashMap();
        properties.put(KinesisReplicatorProvider.ACCESS_KEY_NAME, "a1");
        properties.put(KinesisReplicatorProvider.SECRET_KEY_NAME, "s1");
        ReplicatorPolicies policies = new ReplicatorPolicies();
        policies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();
        policies.replicationProperties = properties;
        // empty topic name mapping
        provider.validateProperties(null, policies);

        policies.topicNameMapping = Maps.newHashMap();
        policies.topicNameMapping.put("topic1", "invalid-stram-name");
        try {
            provider.validateProperties(null, policies);
            Assert.fail("stream name must be in format: <stream-name>:<region-name>");
        } catch (IllegalArgumentException ie) {// expected
        }

        policies.topicNameMapping.put("topic1", "streamName:regionName");
        provider.validateProperties(null, policies);
    }

    @Test
    public void testCreateProducer() throws Exception {
        KinesisReplicatorProvider provider = KinesisReplicatorProvider.instance();
        final String topicName = "persistent://property/cluster/namespace/topic1";
        ReplicatorPolicies policies = new ReplicatorPolicies();
        Map<String, String> properties = Maps.newHashMap();
        properties.put(KinesisReplicatorProvider.ACCESS_KEY_NAME, "a1");
        properties.put(KinesisReplicatorProvider.SECRET_KEY_NAME, "s1");
        policies.replicationProperties = properties;
        policies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();
        policies.topicNameMapping = Maps.newHashMap();
        
        try {
            provider.validateProperties(null, policies);
        }catch (IllegalArgumentException e) {// expected
        }
        
        policies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();
        provider.validateProperties(null, policies);
        
        policies.topicNameMapping.put("topic1", "invalid-stream-name");
        try {
            provider.createProducerAsync(topicName, policies).get();
            Assert.fail("stream name must be in format: <stream-name>:<region-name>");
        } catch (IllegalArgumentException e) {// expected
        }

        policies.topicNameMapping.put("topic2", "streamName:us-west-2");
        try {
            provider.createProducerAsync(topicName, policies).get();
            Assert.fail("topic-name is missing for a " + topicName);
        } catch (IllegalArgumentException e) {// expected
        }

        policies.topicNameMapping.put("topic1", "streamName:us-west-2");
        provider.createProducerAsync(topicName, policies).get();
    }
}
