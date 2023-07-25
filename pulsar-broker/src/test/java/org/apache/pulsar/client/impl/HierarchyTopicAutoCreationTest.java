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
package org.apache.pulsar.client.impl;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
@Slf4j
public class HierarchyTopicAutoCreationTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(invocationCount = 3)
    @SneakyThrows
    public void testPartitionedTopicAutoCreation() {
        // Create namespace
        final String namespace = "public/testPartitionedTopicAutoCreation";
        admin.namespaces().createNamespace(namespace);
        // Set policies
        final AutoTopicCreationOverride expectedPolicies = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType("partitioned")
                .defaultNumPartitions(1)
                .build();
        admin.namespaces().setAutoTopicCreation(namespace, expectedPolicies);
        // Double-check the policies
        final AutoTopicCreationOverride nsAutoTopicCreationOverride = admin.namespaces()
                .getAutoTopicCreation(namespace);
        Assert.assertEquals(nsAutoTopicCreationOverride, expectedPolicies);
        // Background invalidate cache
        final MetadataCache<Policies> nsCache = pulsar.getPulsarResources().getNamespaceResources().getCache();
        final Thread t1 = new Thread(() -> {
            while (true) {
                nsCache.invalidate("/admin/policies/" + namespace);
            }
        });
        t1.start();

        // trigger auto-creation
        final String topicName = "persistent://" + namespace + "/test-" + UUID.randomUUID();
        @Cleanup final Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        final List<String> topics = admin.topics().getList(namespace);
        Assert.assertEquals(topics.size(), 1);  // expect only one topic
        Assert.assertEquals(topics.get(0),
                TopicName.get(topicName).getPartition(0).toString()); // expect partitioned topic

        // double-check policies
        final AutoTopicCreationOverride actualPolicies2 = admin.namespaces().getAutoTopicCreation(namespace);
        Assert.assertEquals(actualPolicies2, expectedPolicies);

        t1.interrupt();
    }
}