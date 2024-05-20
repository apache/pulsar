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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
public class GetPartitionMetadataTest extends ProducerConsumerBase {

    private static final String DEFAULT_NS = "public/default";

    private PulsarClientImpl clientWithHttpLookup;
    private PulsarClientImpl clientWitBinaryLookup;

    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        clientWithHttpLookup =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        clientWitBinaryLookup =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (clientWithHttpLookup != null) {
            clientWithHttpLookup.close();
        }
        if (clientWitBinaryLookup != null) {
            clientWitBinaryLookup.close();
        }
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
    }

    private LookupService getLookupService(boolean isUsingHttpLookup) {
        if (isUsingHttpLookup) {
            return clientWithHttpLookup.getLookup();
        } else {
            return clientWitBinaryLookup.getLookup();
        }
    }

    @Test
    public void testAutoCreatingMetadataWhenCallingOldAPI() throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(true);
        setup();

        // HTTP client.
        final String tp1 = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NS + "/tp");
        clientWithHttpLookup.getPartitionsForTopic(tp1).join();
        Optional<PartitionedTopicMetadata> metadata1 = pulsar.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(tp1), true).join();
        assertTrue(metadata1.isPresent());
        assertEquals(metadata1.get().partitions, 3);

        // Binary client.
        final String tp2 = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NS + "/tp");
        clientWitBinaryLookup.getPartitionsForTopic(tp2).join();
        Optional<PartitionedTopicMetadata> metadata2 = pulsar.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(tp2), true).join();
        assertTrue(metadata2.isPresent());
        assertEquals(metadata2.get().partitions, 3);

        // Cleanup.
        admin.topics().deletePartitionedTopic(tp1, false);
        admin.topics().deletePartitionedTopic(tp2, false);
    }

    @DataProvider(name = "autoCreationParamsAll")
    public Object[][] autoCreationParamsAll(){
        return new Object[][]{
            // configAllowAutoTopicCreation, paramCreateIfAutoCreationEnabled, isUsingHttpLookup.
            {true, true, true},
            {true, true, false},
            {true, false, true},
            {true, false, false},
            {false, true, true},
            {false, true, false},
            {false, false, true},
            {false, false, false}
        };
    }

    @Test(dataProvider = "autoCreationParamsAll")
    public void testGetMetadataIfNonPartitionedTopicExists(boolean configAllowAutoTopicCreation,
                                                           boolean paramMetadataAutoCreationEnabled,
                                                            boolean isUsingHttpLookup) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(configAllowAutoTopicCreation);
        setup();
        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NS + "/tp");
        final TopicName topicName = TopicName.get(topicNameStr);
        admin.topics().createNonPartitionedTopic(topicNameStr);
        // Verify.
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        PartitionedTopicMetadata response =
                lookup.getPartitionedTopicMetadata(topicName, paramMetadataAutoCreationEnabled).join();
        assertEquals(response.partitions, 0);
        List<String> partitionedTopics = admin.topics().getPartitionedTopicList("public/default");
        assertFalse(partitionedTopics.contains(topicNameStr));
        List<String> topicList = admin.topics().getList("public/default");
        for (int i = 0; i < 3; i++) {
            assertFalse(topicList.contains(topicName.getPartition(i)));
        }
        // Cleanup.
        client.close();
        admin.topics().delete(topicNameStr, false);
    }

    @Test(dataProvider = "autoCreationParamsAll")
    public void testGetMetadataIfPartitionedTopicExists(boolean configAllowAutoTopicCreation,
                                                        boolean paramMetadataAutoCreationEnabled,
                                                        boolean isUsingHttpLookup) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(configAllowAutoTopicCreation);
        setup();
        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NS + "/tp");
        final TopicName topicName = TopicName.get(topicNameStr);
        admin.topics().createPartitionedTopic(topicNameStr, 3);
        // Verify.
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        PartitionedTopicMetadata response =
                lookup.getPartitionedTopicMetadata(topicName, paramMetadataAutoCreationEnabled).join();
        assertEquals(response.partitions, 3);
        List<String> topicList = admin.topics().getList("public/default");
        assertFalse(topicList.contains(topicNameStr));
        // Cleanup.
        client.close();
        admin.topics().deletePartitionedTopic(topicNameStr, false);
    }

    @DataProvider(name = "clients")
    public Object[][] clients(){
        return new Object[][]{
                // isUsingHttpLookup.
                {true},
                {false}
        };
    }

    @Test(dataProvider = "clients")
    public void testAutoCreatePartitionedTopic(boolean isUsingHttpLookup) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(true);
        setup();
        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NS + "/tp");
        final TopicName topicName = TopicName.get(topicNameStr);
        // Verify.
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        PartitionedTopicMetadata response = lookup.getPartitionedTopicMetadata(topicName, true).join();
        assertEquals(response.partitions, 3);
        List<String> partitionedTopics = admin.topics().getPartitionedTopicList("public/default");
        assertTrue(partitionedTopics.contains(topicNameStr));
        List<String> topicList = admin.topics().getList("public/default");
        assertFalse(topicList.contains(topicNameStr));
        for (int i = 0; i < 3; i++) {
            // The API "getPartitionedTopicMetadata" only creates the partitioned metadata, it will not create the
            // partitions.
            assertFalse(topicList.contains(topicName.getPartition(i)));
        }
        // Cleanup.
        client.close();
        admin.topics().deletePartitionedTopic(topicNameStr, false);
    }

    @Test(dataProvider = "clients")
    public void testAutoCreateNonPartitionedTopic(boolean isUsingHttpLookup) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        conf.setAllowAutoTopicCreation(true);
        setup();
        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NS + "/tp");
        final TopicName topicName = TopicName.get(topicNameStr);
        // Verify.
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        PartitionedTopicMetadata response = lookup.getPartitionedTopicMetadata(topicName, true).join();
        assertEquals(response.partitions, 0);
        List<String> partitionedTopics = admin.topics().getPartitionedTopicList("public/default");
        assertFalse(partitionedTopics.contains(topicNameStr));
        List<String> topicList = admin.topics().getList("public/default");
        assertFalse(topicList.contains(topicNameStr));
        // Cleanup.
        client.close();
    }

    @DataProvider(name = "autoCreationParamsNotAllow")
    public Object[][] autoCreationParamsNotAllow(){
        return new Object[][]{
                // configAllowAutoTopicCreation, paramCreateIfAutoCreationEnabled, isUsingHttpLookup.
                {true, false, true},
                {true, false, false},
                {false, true, true},
                {false, true, false},
                {false, false, true},
                {false, false, false},
        };
    }

    @Test(dataProvider = "autoCreationParamsNotAllow")
    public void testGetMetadataIfNotAllowedCreate(boolean configAllowAutoTopicCreation,
                                                  boolean paramMetadataAutoCreationEnabled,
                                                  boolean isUsingHttpLookup) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(configAllowAutoTopicCreation);
        setup();
        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Define topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NS + "/tp");
        final TopicName topicName = TopicName.get(topicNameStr);
        // Verify.
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        try {
            lookup.getPartitionedTopicMetadata(TopicName.get(topicNameStr), paramMetadataAutoCreationEnabled).join();
            fail("Expect a not found exception");
        } catch (Exception e) {
            log.warn("", e);
            Throwable unwrapEx = FutureUtil.unwrapCompletionException(e);
            assertTrue(unwrapEx instanceof PulsarClientException.TopicDoesNotExistException
                    || unwrapEx instanceof PulsarClientException.NotFoundException);
        }
        List<String> partitionedTopics = admin.topics().getPartitionedTopicList("public/default");
        assertFalse(partitionedTopics.contains(topicNameStr));
        List<String> topicList = admin.topics().getList("public/default");
        assertFalse(topicList.contains(topicNameStr));
        for (int i = 0; i < 3; i++) {
            assertFalse(topicList.contains(topicName.getPartition(i)));
        }
        // Cleanup.
        client.close();
    }
}
