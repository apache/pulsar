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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@CustomLog
public class LookupServiceTest extends SharedPulsarBaseTest {

    private PulsarClientImpl clientWithHttpLookup;
    private PulsarClientImpl clientWitBinaryLookup;

    @Override
    @BeforeClass(alwaysRun = true)
    public void setupSharedCluster() throws Exception {
        super.setupSharedCluster();
        clientWithHttpLookup =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(getWebServiceUrl()).build();
        clientWitBinaryLookup =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(getBrokerServiceUrl()).build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanupClients() throws Exception {
        if (clientWithHttpLookup != null) {
            clientWithHttpLookup.close();
        }
        if (clientWitBinaryLookup != null) {
            clientWitBinaryLookup.close();
        }
    }

    private LookupService getLookupService(boolean isUsingHttpLookup) {
        if (isUsingHttpLookup) {
            return clientWithHttpLookup.getLookup();
        } else {
            return clientWitBinaryLookup.getLookup();
        }
    }

    @DataProvider(name = "isUsingHttpLookup")
    public Object[][] isUsingHttpLookup() {
        return new Object[][]{
            {true},
            {false}
        };
    }

    @Test(dataProvider = "isUsingHttpLookup")
    public void testGetTopicsOfGetTopicsResult(boolean isUsingHttpLookup) throws Exception {
        LookupService lookupService = getLookupService(isUsingHttpLookup);
        String nonPartitionedTopic = newTopicName();
        admin.topics().createNonPartitionedTopic(nonPartitionedTopic);
        String partitionedTopic = newTopicName();
        admin.topics().createPartitionedTopic(partitionedTopic, 3);

        String ns = getNamespace();

        // Verify the new method "GetTopicsResult.getTopics" works as expected.
        Collection<String> topics = lookupService.getTopicsUnderNamespace(NamespaceName.get(ns),
                Mode.PERSISTENT, ns + "/.*", null).join().getTopics();
        assertTrue(topics.contains(nonPartitionedTopic));
        assertTrue(topics.contains(partitionedTopic));
        assertFalse(topics.contains(TopicName.get(partitionedTopic).getPartition(0).toString()));
        // Verify the new method "GetTopicsResult.nonPartitionedOrPartitionTopics" works as expected.
        Collection<String> nonPartitionedOrPartitionTopics =
                lookupService.getTopicsUnderNamespace(NamespaceName.get(ns),
                Mode.PERSISTENT, ns + "/.*", null).join()
                .getNonPartitionedOrPartitionTopics();
        assertTrue(nonPartitionedOrPartitionTopics.contains(nonPartitionedTopic));
        assertFalse(nonPartitionedOrPartitionTopics.contains(partitionedTopic));
        assertTrue(nonPartitionedOrPartitionTopics.contains(TopicName.get(partitionedTopic).getPartition(0)
                .toString()));
        assertTrue(nonPartitionedOrPartitionTopics.contains(TopicName.get(partitionedTopic).getPartition(1)
                .toString()));
        assertTrue(nonPartitionedOrPartitionTopics.contains(TopicName.get(partitionedTopic).getPartition(2)
                .toString()));
    }

    @Test(dataProvider = "isUsingHttpLookup")
    public void testGetPartitionedTopicMetadataByPulsarClient(boolean isUsingHttpLookup) throws PulsarAdminException {
        LookupService lookupService = getLookupService(isUsingHttpLookup);

        // metadataAutoCreationEnabled is true.
        assertThat(lookupService.getPartitionedTopicMetadata(
                TopicName.get(newTopicName()), true))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(n -> n.partitions == 0);

        // metadataAutoCreationEnabled is true.
        // Allow the get the metadata of single partition topic, because the auto-creation is enabled.
        // But the producer/consumer is unavailable because the topic doesn't have the metadata.
        assertThat(lookupService.getPartitionedTopicMetadata(
                TopicName.get(newTopicName() + "-partition-10"),
                true))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(n -> n.partitions == 0);

        Class<? extends Throwable> expectedExceptionClass =
                isUsingHttpLookup ? PulsarClientException.NotFoundException.class :
                        PulsarClientException.TopicDoesNotExistException.class;
        // metadataAutoCreationEnabled is false.
        assertThat(lookupService.getPartitionedTopicMetadata(
                TopicName.get(newTopicName()), false))
                .failsWithin(3, TimeUnit.SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(expectedExceptionClass);

        // metadataAutoCreationEnabled is false.
        assertThat(lookupService.getPartitionedTopicMetadata(
                TopicName.get(newTopicName() + "-partition-10"),
                false))
                .failsWithin(3, TimeUnit.SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(expectedExceptionClass);

        // Verify the topic exists, and the metadataAutoCreationEnabled is false.
        String nonPartitionedTopic = newTopicName();
        admin.topics().createNonPartitionedTopic(nonPartitionedTopic);
        assertThat(lookupService.getPartitionedTopicMetadata(TopicName.get(nonPartitionedTopic), false))
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(n -> n.partitions == 0);

        String partitionedTopic = newTopicName();
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
        String nonPartitionedTopic = newTopicName();
        String partitionedTopic = newTopicName();
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
}
