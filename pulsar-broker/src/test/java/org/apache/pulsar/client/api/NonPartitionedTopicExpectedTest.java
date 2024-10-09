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
package org.apache.pulsar.client.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class NonPartitionedTopicExpectedTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testWhenNonPartitionedTopicExists() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topic);
        ProducerBuilderImpl<String> producerBuilder =
                (ProducerBuilderImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topic);
        producerBuilder.getConf().setNonPartitionedTopicExpected(true);
        // Verify: create successfully.
        Producer producer = producerBuilder.create();
        // cleanup.
        producer.close();
        admin.topics().delete(topic, false);
    }

    @Test
    public void testWhenPartitionedTopicExists() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createPartitionedTopic(topic, 2);
        ProducerBuilderImpl<String> producerBuilder =
                (ProducerBuilderImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topic);
        producerBuilder.getConf().setNonPartitionedTopicExpected(true);
        // Verify: failed to create.
        try {
            producerBuilder.create();
            Assert.fail("expected an error since producer expected a non-partitioned topic");
        } catch (Exception ex) {
            // expected an error.
            log.error("expected error", ex);
        }
        // cleanup.
        admin.topics().deletePartitionedTopic(topic, false);
    }

    @DataProvider(name = "topicTypes")
    public Object[][] topicTypes() {
        return new Object[][]{
            {TopicType.PARTITIONED},
            {TopicType.NON_PARTITIONED}
        };
    }

    @Test(dataProvider = "topicTypes")
    public void testWhenTopicNotExists(TopicType topicType) throws Exception {
        final String namespace = "public/default";
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp");
        final TopicName topicName = TopicName.get(topic);
        AutoTopicCreationOverride.Builder policyBuilder = AutoTopicCreationOverride.builder()
                .topicType(topicType.toString()).allowAutoTopicCreation(true);
        if (topicType.equals(TopicType.PARTITIONED)) {
            policyBuilder.defaultNumPartitions(2);
        }
        AutoTopicCreationOverride policy = policyBuilder.build();
        admin.namespaces().setAutoTopicCreation(namespace, policy);

        ProducerBuilderImpl<String> producerBuilder =
                (ProducerBuilderImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topic);
        producerBuilder.getConf().setNonPartitionedTopicExpected(true);
        // Verify: create successfully.
        Producer producer = producerBuilder.create();
        // Verify: only create non-partitioned topic.
        Assert.assertFalse(pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .partitionedTopicExists(topicName));
        Assert.assertTrue(pulsar.getNamespaceService().checkNonPartitionedTopicExists(topicName).join());

        // cleanup.
        producer.close();
        admin.topics().delete(topic, false);
        admin.namespaces().removeAutoTopicCreation(namespace);
    }
}
