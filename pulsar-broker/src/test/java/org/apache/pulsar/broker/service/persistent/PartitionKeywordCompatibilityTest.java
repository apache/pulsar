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
package org.apache.pulsar.broker.service.persistent;


import static org.testng.Assert.fail;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.List;

@Test
public class PartitionKeywordCompatibilityTest extends BrokerTestBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        baseSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    public void testAutoCreatePartitionTopicWithKeywordAndDeleteIt()
            throws PulsarAdminException, PulsarClientException {
        AutoTopicCreationOverride override = AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType("partitioned")
                .defaultNumPartitions(1)
                .build();
        admin.namespaces().setAutoTopicCreation("public/default", override);
        String topicName = "persistent://public/default/XXX-partition-0-dd";
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("sub-1")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        List<String> topics = admin.topics().getList("public/default");
        List<String> partitionedTopicList = admin.topics().getPartitionedTopicList("public/default");
        Assert.assertTrue(topics.contains(topicName));
        Assert.assertTrue(partitionedTopicList.contains(topicName));
        consumer.close();
        admin.topics().deletePartitionedTopic(topicName);
        topics = admin.topics().getList("public/default");
        partitionedTopicList = admin.topics().getPartitionedTopicList("public/default");
        Assert.assertFalse(topics.contains(topicName));
        Assert.assertFalse(partitionedTopicList.contains(topicName));
    }

    @Test
    public void testDeletePartitionedTopicValidation() throws PulsarAdminException {
        final String topicName = "persistent://public/default/testDeletePartitionedTopicValidation";
        final String partitionKeywordTopic = "persistent://public/default/testDelete-partition-edTopicValidation";
        final String partitionedTopic = "persistent://public/default/testDeletePartitionedTopicValidation-partition-0";
        try {
            admin.topics().deletePartitionedTopic(topicName);
            fail("expect not found!");
        } catch (PulsarAdminException.NotFoundException ex) {
            //ok
        }
        try {
            admin.topics().deletePartitionedTopic(partitionKeywordTopic);
            fail("expect not found!");
        } catch (PulsarAdminException.NotFoundException ex) {
            //ok
        }
        try {
            admin.topics().deletePartitionedTopic(partitionedTopic);
            fail("expect illegal argument");
        } catch (PulsarAdminException.PreconditionFailedException ex) {
            Assert.assertTrue(ex.getMessage().contains("should not contain '-partition-'"));
            // ok
        }
    }
}
