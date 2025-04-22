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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException.NotAllowedException;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ValidatePartitionMetadataConsistencyTest extends ProducerConsumerBase {

    private PulsarClientImpl clientWithHttpLookup;
    private PulsarClientImpl clientWithBinaryLookup;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setValidatePartitionMetadataConsistency(true);
        super.internalSetup();
        super.producerBaseSetup();
        clientWithHttpLookup =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        clientWithBinaryLookup =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (clientWithHttpLookup != null) {
            clientWithHttpLookup.close();
        }
        if (clientWithBinaryLookup != null) {
            clientWithBinaryLookup.close();
        }
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

    @Test(dataProvider = "topicDomainProvider")
    public void testCreateProducerWhenTopicTypeMismatch(TopicDomain domain)
            throws PulsarAdminException, PulsarClientException {
        String nonPartitionedTopic =
                TopicName.get(domain.value(), "public", "default",
                                "testCreateProducerWhenTopicTypeMismatch-nonPartitionedTopic")
                        .toString();
        admin.topics().createNonPartitionedTopic(nonPartitionedTopic);

        // Topic type is non-partitioned, trying to create producer on the complete partitioned topic.
        // Should throw NotAllowedException.
        assertThrows(PulsarClientException.NotAllowedException.class, () -> {
            @Cleanup
            Producer<byte[]> ignored =
                    pulsarClient.newProducer().topic(TopicName.get(nonPartitionedTopic).getPartition(2).toString())
                            .create();
        });

        // Topic type is partitioned, trying to create producer on the base partitioned topic.
        String partitionedTopic = TopicName.get(domain.value(), "public", "default",
                        "testCreateProducerWhenTopicTypeMismatch-partitionedTopic")
                .toString();
        admin.topics().createPartitionedTopic(partitionedTopic, 3);

        // Works fine because the lookup can help our to find all the topics.
        {
            @Cleanup
            Producer<byte[]> ignored =
                    pulsarClient.newProducer().topic(TopicName.get(partitionedTopic).getPartitionedTopicName())
                            .create();
        }

        // Partition index is out of range.
        assertThrows(PulsarClientException.NotAllowedException.class, () -> {
            @Cleanup
            Producer<byte[]> ignored =
                    pulsarClient.newProducer().topic(TopicName.get(partitionedTopic).getPartition(100).toString())
                            .create();
        });
    }

    @DataProvider(name = "isUsingHttpLookup")
    public Object[][] isUsingHttpLookup() {
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(dataProvider = "isUsingHttpLookup")
    public void testGetPartitionedTopicMetadataByPulsarClient(boolean isUsingHttpLookup) throws PulsarAdminException {
        LookupService lookupService = getLookupService(isUsingHttpLookup);

        // metadataAutoCreationEnabled is true.
        assertThat(lookupService.getPartitionedTopicMetadata(
                TopicName.get(BrokerTestUtil.newUniqueName("persistent://public/default/tp")), true))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(n -> n.partitions == 0);

        // metadataAutoCreationEnabled is true.
        // Allow the get the metadata of single partition topic, because the auto-creation is enabled.
        // But the producer/consumer is unavailable because the topic doesn't have the metadata.
        assertThat(lookupService.getPartitionedTopicMetadata(
                TopicName.get(BrokerTestUtil.newUniqueName("persistent://public/default/tp") + "-partition-10"),
                true))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(n -> n.partitions == 0);

        Class<? extends Throwable> expectedExceptionClass =
                isUsingHttpLookup ? PulsarClientException.NotFoundException.class :
                        PulsarClientException.TopicDoesNotExistException.class;
        // metadataAutoCreationEnabled is false.
        assertThat(lookupService.getPartitionedTopicMetadata(
                TopicName.get(BrokerTestUtil.newUniqueName("persistent://public/default/tp")), false))
                .failsWithin(3, TimeUnit.SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(expectedExceptionClass);

        // metadataAutoCreationEnabled is false.
        assertThat(lookupService.getPartitionedTopicMetadata(
                TopicName.get(BrokerTestUtil.newUniqueName("persistent://public/default/tp") + "-partition-10"),
                false))
                .failsWithin(3, TimeUnit.SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(expectedExceptionClass);

        // Verify the topic exists, and the metadataAutoCreationEnabled is false.
        String nonPartitionedTopic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(nonPartitionedTopic);
        assertThat(lookupService.getPartitionedTopicMetadata(TopicName.get(nonPartitionedTopic), false))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(n -> n.partitions == 0);

        String partitionedTopic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String partitionedTopicWithPartitionIndex = partitionedTopic + "-partition-10";
        admin.topics().createPartitionedTopic(partitionedTopic, 20);
        assertThat(lookupService.getPartitionedTopicMetadata(TopicName.get(partitionedTopic), false))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(n -> n.partitions == 20);
        assertThat(lookupService.getPartitionedTopicMetadata(TopicName.get(partitionedTopicWithPartitionIndex), false))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(n -> n.partitions == 0);
    }

    @Test
    public void testGetPartitionedTopicMedataByAdmin() throws PulsarAdminException {
        String nonPartitionedTopic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String partitionedTopic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String partitionedTopicWithPartitionIndex = partitionedTopic + "-partition-10";
        // No topic, so throw the NotFound.
        // BTW: The admin api doesn't allow to creat the metadata of topic default.
        assertThrows(PulsarAdminException.NotFoundException.class, () -> admin.topics()
                .getPartitionedTopicMetadata(nonPartitionedTopic));
        assertThrows(PulsarAdminException.NotFoundException.class, () -> admin.topics()
                .getPartitionedTopicMetadata(partitionedTopic));
        assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.topics().getPartitionedTopicMetadata(partitionedTopicWithPartitionIndex));

        admin.topics().createNonPartitionedTopic(nonPartitionedTopic);
        assertEquals(admin.topics().getPartitionedTopicMetadata(nonPartitionedTopic).partitions, 0);

        admin.topics().createPartitionedTopic(partitionedTopic, 20);
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopic).partitions, 20);
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicWithPartitionIndex).partitions, 0);
    }

    private LookupService getLookupService(boolean isUsingHttpLookup) {
        if (isUsingHttpLookup) {
            return clientWithHttpLookup.getLookup();
        } else {
            return clientWithBinaryLookup.getLookup();
        }
    }
}
