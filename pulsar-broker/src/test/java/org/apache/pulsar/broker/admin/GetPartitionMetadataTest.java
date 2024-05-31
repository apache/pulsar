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
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
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

    @DataProvider(name = "topicDomains")
    public Object[][] topicDomains() {
        return new Object[][]{
            {TopicDomain.persistent},
            {TopicDomain.non_persistent}
        };
    }

    @Test(dataProvider = "topicDomains")
    public void testAutoCreatingMetadataWhenCallingOldAPI(TopicDomain topicDomain) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(true);
        setup();

        Semaphore semaphore = pulsar.getBrokerService().getLookupRequestSemaphore();
        int lookupPermitsBefore = semaphore.availablePermits();

        // HTTP client.
        final String tp1 = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
        clientWithHttpLookup.getPartitionsForTopic(tp1).join();
        Optional<PartitionedTopicMetadata> metadata1 = pulsar.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(tp1), true).join();
        assertTrue(metadata1.isPresent());
        assertEquals(metadata1.get().partitions, 3);

        // Binary client.
        final String tp2 = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
        clientWitBinaryLookup.getPartitionsForTopic(tp2).join();
        Optional<PartitionedTopicMetadata> metadata2 = pulsar.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(tp2), true).join();
        assertTrue(metadata2.isPresent());
        assertEquals(metadata2.get().partitions, 3);

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            int lookupPermitsAfter = semaphore.availablePermits();
            assertEquals(lookupPermitsAfter, lookupPermitsBefore);
        });

        // Cleanup.
        admin.topics().deletePartitionedTopic(tp1, false);
        admin.topics().deletePartitionedTopic(tp2, false);
    }

    @DataProvider(name = "autoCreationParamsAll")
    public Object[][] autoCreationParamsAll(){
        return new Object[][]{
            // configAllowAutoTopicCreation, paramCreateIfAutoCreationEnabled, isUsingHttpLookup.
            {true, true, true, TopicDomain.persistent},
            {true, true, false, TopicDomain.persistent},
            {true, false, true, TopicDomain.persistent},
            {true, false, false, TopicDomain.persistent},
            {false, true, true, TopicDomain.persistent},
            {false, true, false, TopicDomain.persistent},
            {false, false, true, TopicDomain.persistent},
            {false, false, false, TopicDomain.persistent},
            {true, true, true, TopicDomain.non_persistent},
            {true, true, false, TopicDomain.non_persistent},
            {true, false, true, TopicDomain.non_persistent},
            {true, false, false, TopicDomain.non_persistent},
            {false, true, true, TopicDomain.non_persistent},
            {false, true, false, TopicDomain.non_persistent},
            {false, false, true, TopicDomain.non_persistent},
            {false, false, false, TopicDomain.non_persistent}
        };
    }

    @Test(dataProvider = "autoCreationParamsAll")
    public void testGetMetadataIfNonPartitionedTopicExists(boolean configAllowAutoTopicCreation,
                                                           boolean paramMetadataAutoCreationEnabled,
                                                           boolean isUsingHttpLookup,
                                                           TopicDomain topicDomain) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(configAllowAutoTopicCreation);
        setup();

        Semaphore semaphore = pulsar.getBrokerService().getLookupRequestSemaphore();
        int lookupPermitsBefore = semaphore.availablePermits();

        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
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

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            int lookupPermitsAfter = semaphore.availablePermits();
            assertEquals(lookupPermitsAfter, lookupPermitsBefore);
        });

        // Cleanup.
        client.close();
        admin.topics().delete(topicNameStr, false);
    }

    @Test(dataProvider = "autoCreationParamsAll")
    public void testGetMetadataIfPartitionedTopicExists(boolean configAllowAutoTopicCreation,
                                                        boolean paramMetadataAutoCreationEnabled,
                                                        boolean isUsingHttpLookup,
                                                        TopicDomain topicDomain) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(configAllowAutoTopicCreation);
        setup();

        Semaphore semaphore = pulsar.getBrokerService().getLookupRequestSemaphore();
        int lookupPermitsBefore = semaphore.availablePermits();

        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
        final TopicName topicName = TopicName.get(topicNameStr);
        admin.topics().createPartitionedTopic(topicNameStr, 3);
        // Verify.
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        PartitionedTopicMetadata response =
                lookup.getPartitionedTopicMetadata(topicName, paramMetadataAutoCreationEnabled).join();
        assertEquals(response.partitions, 3);
        List<String> topicList = admin.topics().getList("public/default");
        assertFalse(topicList.contains(topicNameStr));

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            int lookupPermitsAfter = semaphore.availablePermits();
            assertEquals(lookupPermitsAfter, lookupPermitsBefore);
        });

        // Cleanup.
        client.close();
        admin.topics().deletePartitionedTopic(topicNameStr, false);
    }

    @DataProvider(name = "clients")
    public Object[][] clients(){
        return new Object[][]{
                // isUsingHttpLookup.
                {true, TopicDomain.persistent},
                {false, TopicDomain.non_persistent}
        };
    }

    @Test(dataProvider = "clients")
    public void testAutoCreatePartitionedTopic(boolean isUsingHttpLookup, TopicDomain topicDomain) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(true);
        setup();

        Semaphore semaphore = pulsar.getBrokerService().getLookupRequestSemaphore();
        int lookupPermitsBefore = semaphore.availablePermits();

        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
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

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            int lookupPermitsAfter = semaphore.availablePermits();
            assertEquals(lookupPermitsAfter, lookupPermitsBefore);
        });

        // Cleanup.
        client.close();
        admin.topics().deletePartitionedTopic(topicNameStr, false);
    }

    @Test(dataProvider = "clients")
    public void testAutoCreateNonPartitionedTopic(boolean isUsingHttpLookup, TopicDomain topicDomain) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        conf.setAllowAutoTopicCreation(true);
        setup();

        Semaphore semaphore = pulsar.getBrokerService().getLookupRequestSemaphore();
        int lookupPermitsBefore = semaphore.availablePermits();

        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
        final TopicName topicName = TopicName.get(topicNameStr);
        // Verify.
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        PartitionedTopicMetadata response = lookup.getPartitionedTopicMetadata(topicName, true).join();
        assertEquals(response.partitions, 0);
        List<String> partitionedTopics = admin.topics().getPartitionedTopicList("public/default");
        assertFalse(partitionedTopics.contains(topicNameStr));
        List<String> topicList = admin.topics().getList("public/default");
        assertFalse(topicList.contains(topicNameStr));

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            int lookupPermitsAfter = semaphore.availablePermits();
            assertEquals(lookupPermitsAfter, lookupPermitsBefore);
        });

        // Cleanup.
        client.close();
        try {
            admin.topics().delete(topicNameStr, false);
        } catch (Exception ex) {}
    }

    @DataProvider(name = "autoCreationParamsNotAllow")
    public Object[][] autoCreationParamsNotAllow(){
        return new Object[][]{
                // configAllowAutoTopicCreation, paramCreateIfAutoCreationEnabled, isUsingHttpLookup.
                {true, false, true},
                {true, false, false},
                {false, false, true},
                {false, false, false},
                {false, true, true},
                {false, true, false},
        };
    }

    @Test(dataProvider = "autoCreationParamsNotAllow")
    public void testGetMetadataIfNotAllowedCreate(boolean configAllowAutoTopicCreation,
                                                  boolean paramMetadataAutoCreationEnabled,
                                                  boolean isUsingHttpLookup) throws Exception {
        if (!configAllowAutoTopicCreation && paramMetadataAutoCreationEnabled) {
            // These test cases are for the following PR.
            // Which was described in the Motivation of https://github.com/apache/pulsar/pull/22206.
            return;
        }
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(configAllowAutoTopicCreation);
        setup();

        Semaphore semaphore = pulsar.getBrokerService().getLookupRequestSemaphore();
        int lookupPermitsBefore = semaphore.availablePermits();

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
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources().partitionedTopicExists(topicName);
        assertFalse(partitionedTopics.contains(topicNameStr));
        List<String> topicList = admin.topics().getList("public/default");
        assertFalse(topicList.contains(topicNameStr));
        for (int i = 0; i < 3; i++) {
            assertFalse(topicList.contains(topicName.getPartition(i)));
        }

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            int lookupPermitsAfter = semaphore.availablePermits();
            assertEquals(lookupPermitsAfter, lookupPermitsBefore);
        });

        // Cleanup.
        client.close();
    }

    @DataProvider(name = "autoCreationParamsForNonPersistentTopic")
    public Object[][] autoCreationParamsForNonPersistentTopic(){
        return new Object[][]{
                // configAllowAutoTopicCreation, paramCreateIfAutoCreationEnabled, isUsingHttpLookup.
                {true, true, true},
                {true, true, false},
                {false, true, true},
                {false, true, false},
                {false, false, true}
        };
    }

    /**
     * Regarding the API "get partitioned metadata" about non-persistent topic.
     * The original behavior is:
     *   param-auto-create = true, broker-config-auto-create = true
     *     HTTP API: default configuration {@link ServiceConfiguration#getDefaultNumPartitions()}
     *     binary API: default configuration {@link ServiceConfiguration#getDefaultNumPartitions()}
     *   param-auto-create = true, broker-config-auto-create = false
     *     HTTP API: {partitions: 0}
     *     binary API: {partitions: 0}
     *   param-auto-create = false
     *     HTTP API: not found error
     *     binary API: not support
     *  This test only guarantees that the behavior is the same as before. The following separated PR will fix the
     *  incorrect behavior.
     */
    @Test(dataProvider = "autoCreationParamsForNonPersistentTopic")
    public void testGetNonPersistentMetadataIfNotAllowedCreate(boolean configAllowAutoTopicCreation,
                                                  boolean paramMetadataAutoCreationEnabled,
                                                  boolean isUsingHttpLookup) throws Exception {
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreation(configAllowAutoTopicCreation);
        setup();

        Semaphore semaphore = pulsar.getBrokerService().getLookupRequestSemaphore();
        int lookupPermitsBefore = semaphore.availablePermits();

        LookupService lookup = getLookupService(isUsingHttpLookup);
        // Define topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName("non-persistent://" + DEFAULT_NS + "/tp");
        final TopicName topicName = TopicName.get(topicNameStr);
        // Verify.
        // Regarding non-persistent topic, we do not know whether it exists or not.
        // Broker will return a non-partitioned metadata if partitioned metadata does not exist.
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();

        if (!configAllowAutoTopicCreation && !paramMetadataAutoCreationEnabled && isUsingHttpLookup) {
            try {
                lookup.getPartitionedTopicMetadata(TopicName.get(topicNameStr), paramMetadataAutoCreationEnabled)
                        .join();
                Assert.fail("Expected a not found ex");
            } catch (Exception ex) {
                // Cleanup.
                client.close();
                return;
            }
        }

        PartitionedTopicMetadata metadata = lookup
                .getPartitionedTopicMetadata(TopicName.get(topicNameStr), paramMetadataAutoCreationEnabled).join();
        if (configAllowAutoTopicCreation && paramMetadataAutoCreationEnabled) {
            assertEquals(metadata.partitions, 3);
        } else {
            assertEquals(metadata.partitions, 0);
        }

        List<String> partitionedTopics = admin.topics().getPartitionedTopicList("public/default");
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .partitionedTopicExists(topicName);
        if (configAllowAutoTopicCreation && paramMetadataAutoCreationEnabled) {
            assertTrue(partitionedTopics.contains(topicNameStr));
        } else {
            assertFalse(partitionedTopics.contains(topicNameStr));
        }

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            int lookupPermitsAfter = semaphore.availablePermits();
            assertEquals(lookupPermitsAfter, lookupPermitsBefore);
        });

        // Cleanup.
        client.close();
    }
}
