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

import static org.testng.Assert.assertThrows;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException.NotAllowedException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ConsumerCreationTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "topicDomainProvider")
    public Object[][] topicDomainProvider() {
        return new Object[][]{
                {TopicDomain.persistent},
                {TopicDomain.non_persistent}
        };
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testCreateConsumerWhenTopicTypeMismatch(TopicDomain domain)
            throws PulsarAdminException, PulsarClientException {
        String nonPartitionedTopic =
                TopicName.get(domain.value(), "public", "default",
                                "testCreateConsumerWhenTopicTypeMismatch-nonPartitionedTopic")
                        .toString();
        admin.topics().createNonPartitionedTopic(nonPartitionedTopic);

        // Topic type is non-partitioned, Trying to create consumer on partitioned topic.
        assertThrows(NotAllowedException.class, () -> {
            @Cleanup
            Consumer<byte[]> ignored =
                    pulsarClient.newConsumer().topic(TopicName.get(nonPartitionedTopic).getPartition(2).toString())
                            .subscriptionName("my-sub").subscribe();
        });

        // Topic type is partitioned, Trying to create consumer on non-partitioned topic.
        String partitionedTopic = TopicName.get(domain.value(), "public", "default",
                        "testCreateConsumerWhenTopicTypeMismatch-partitionedTopic")
                .toString();
        admin.topics().createPartitionedTopic(partitionedTopic, 3);

        // Works fine because the lookup can help our to find the correct topic.
        {
            @Cleanup
            Consumer<byte[]> ignored =
                    pulsarClient.newConsumer().topic(TopicName.get(partitionedTopic).getPartition(2).toString())
                            .subscriptionName("my-sub").subscribe();
        }

        // Partition index is out of range.
        assertThrows(NotAllowedException.class, () -> {
            @Cleanup
            Consumer<byte[]> ignored =
                    pulsarClient.newConsumer().topic(TopicName.get(partitionedTopic).getPartition(100).toString())
                            .subscriptionName("my-sub").subscribe();
        });
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testCreateConsumerWhenSinglePartitionIsDeleted(TopicDomain domain)
            throws PulsarAdminException, PulsarClientException {
        testCreateConsumerWhenSinglePartitionIsDeleted(domain, false);
        testCreateConsumerWhenSinglePartitionIsDeleted(domain, true);
    }

    private void testCreateConsumerWhenSinglePartitionIsDeleted(TopicDomain domain, boolean allowAutoTopicCreation)
            throws PulsarAdminException, PulsarClientException {
        conf.setAllowAutoTopicCreation(allowAutoTopicCreation);

        String partitionedTopic = TopicName.get(domain.value(), "public", "default",
                        "testCreateConsumerWhenSinglePartitionIsDeleted-" + allowAutoTopicCreation)
                .toString();
        admin.topics().createPartitionedTopic(partitionedTopic, 3);
        admin.topics().delete(TopicName.get(partitionedTopic).getPartition(1).toString());

        // Non-persistent topic only have the metadata, and no partition, so it works fine.
        if (allowAutoTopicCreation || domain.equals(TopicDomain.non_persistent)) {
            @Cleanup
            Consumer<byte[]> ignored =
                    pulsarClient.newConsumer().topic(partitionedTopic).subscriptionName("my-sub").subscribe();
        } else {
            assertThrows(PulsarClientException.NotFoundException.class, () -> {
                @Cleanup
                Consumer<byte[]> ignored =
                        pulsarClient.newConsumer().topic(partitionedTopic).subscriptionName("my-sub").subscribe();
            });
        }
    }
}