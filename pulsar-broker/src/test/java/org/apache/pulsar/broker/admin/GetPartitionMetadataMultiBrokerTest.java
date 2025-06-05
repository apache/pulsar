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
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
public class GetPartitionMetadataMultiBrokerTest extends GetPartitionMetadataTest {

    private PulsarService pulsar2;
    private URL url2;
    private PulsarAdmin admin2;
    private PulsarClientImpl clientWithHttpLookup2;
    private PulsarClientImpl clientWitBinaryLookup2;

    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.cleanup();
    }

    @Override
    protected void cleanupBrokers() throws Exception {
        // Cleanup broker2.
        if (clientWithHttpLookup2 != null) {
            clientWithHttpLookup2.close();
            clientWithHttpLookup2 = null;
        }
        if (clientWitBinaryLookup2 != null) {
            clientWitBinaryLookup2.close();
            clientWitBinaryLookup2 = null;
        }
        if (admin2 != null) {
            admin2.close();
            admin2 = null;
        }
        if (pulsar2 != null) {
            pulsar2.close();
            pulsar2 = null;
        }

        // Super cleanup.
        super.cleanupBrokers();
    }

    @Override
    protected void setupBrokers() throws Exception {
        super.setupBrokers();
        doInitConf();
        pulsar2 = new PulsarService(conf);
        pulsar2.start();
        url2 = new URL(pulsar2.getWebServiceAddress());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();
        clientWithHttpLookup2 =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar2.getWebServiceAddress()).build();
        clientWitBinaryLookup2 =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar2.getBrokerServiceUrl()).build();
    }

    @Override
    protected PulsarClientImpl[] getClientsToTest() {
        return new PulsarClientImpl[] {clientWithHttpLookup1, clientWitBinaryLookup1,
                clientWithHttpLookup2, clientWitBinaryLookup2};
    }

    protected PulsarClientImpl[] getClientsToTest(boolean isUsingHttpLookup) {
        if (isUsingHttpLookup) {
            return new PulsarClientImpl[]{clientWithHttpLookup1, clientWithHttpLookup2};
        } else {
            return new PulsarClientImpl[]{clientWitBinaryLookup1, clientWitBinaryLookup2};
        }
    }

    @Override
    protected int getLookupRequestPermits() {
        return pulsar1.getBrokerService().getLookupRequestSemaphore().availablePermits()
                + pulsar2.getBrokerService().getLookupRequestSemaphore().availablePermits();
    }

    protected void verifyPartitionsNeverCreated(String topicNameStr) throws Exception {
        TopicName topicName = TopicName.get(topicNameStr);
        try {
            List<String> topicList = admin1.topics().getList("public/default");
            for (int i = 0; i < 3; i++) {
                assertFalse(topicList.contains(topicName.getPartition(i)));
            }
        } catch (Exception ex) {
            // If the namespace bundle has not been loaded yet, it means no non-persistent topic was created. So
            //   this behavior is also correct.
            // This error is not expected, a seperated PR is needed to fix this issue.
            assertTrue(ex.getMessage().contains("Failed to find ownership for"));
        }
    }

    protected void verifyNonPartitionedTopicNeverCreated(String topicNameStr) throws Exception {
        TopicName topicName = TopicName.get(topicNameStr);
        try {
            List<String> topicList = admin1.topics().getList("public/default");
            assertFalse(topicList.contains(topicName.getPartitionedTopicName()));
        } catch (Exception ex) {
            // If the namespace bundle has not been loaded yet, it means no non-persistent topic was created. So
            //   this behavior is also correct.
            // This error is not expected, a seperated PR is needed to fix this issue.
            assertTrue(ex.getMessage().contains("Failed to find ownership for"));
        }
    }

    protected void modifyTopicAutoCreation(boolean allowAutoTopicCreation,
                                           TopicType allowAutoTopicCreationType,
                                           int defaultNumPartitions) throws Exception {
        doModifyTopicAutoCreation(admin1, pulsar1, allowAutoTopicCreation, allowAutoTopicCreationType,
                defaultNumPartitions);
        doModifyTopicAutoCreation(admin2, pulsar2, allowAutoTopicCreation, allowAutoTopicCreationType,
                defaultNumPartitions);
    }

    /**
     * {@inheritDoc}
     */
    @Test(dataProvider = "topicDomains")
    public void testAutoCreatingMetadataWhenCallingOldAPI(TopicDomain topicDomain) throws Exception {
        super.testAutoCreatingMetadataWhenCallingOldAPI(topicDomain);
    }

    /**
     * {@inheritDoc}
     */
    @Test(dataProvider = "autoCreationParamsAll", enabled = false)
    public void testGetMetadataIfNonPartitionedTopicExists(boolean configAllowAutoTopicCreation,
                                                           boolean paramMetadataAutoCreationEnabled,
                                                           boolean isUsingHttpLookup,
                                                           TopicDomain topicDomain) throws Exception {
        super.testGetMetadataIfNonPartitionedTopicExists(configAllowAutoTopicCreation, paramMetadataAutoCreationEnabled,
                isUsingHttpLookup, topicDomain);
    }

    /**
     * {@inheritDoc}
     */
    @Test(dataProvider = "autoCreationParamsAll")
    public void testGetMetadataIfPartitionedTopicExists(boolean configAllowAutoTopicCreation,
                                                        boolean paramMetadataAutoCreationEnabled,
                                                        boolean isUsingHttpLookup,
                                                        TopicDomain topicDomain) throws Exception {
        super.testGetMetadataIfNonPartitionedTopicExists(configAllowAutoTopicCreation, paramMetadataAutoCreationEnabled,
                isUsingHttpLookup, topicDomain);
    }

    /**
     * {@inheritDoc}
     */
    @Test(dataProvider = "clients")
    public void testAutoCreatePartitionedTopic(boolean isUsingHttpLookup, TopicDomain topicDomain) throws Exception {
        super.testAutoCreatePartitionedTopic(isUsingHttpLookup, topicDomain);
    }

    /**
     * {@inheritDoc}
     */
    @Test(dataProvider = "clients")
    public void testAutoCreateNonPartitionedTopic(boolean isUsingHttpLookup, TopicDomain topicDomain) throws Exception {
        super.testAutoCreateNonPartitionedTopic(isUsingHttpLookup, topicDomain);
    }

    /**
     * {@inheritDoc}
     */
    @Test(dataProvider = "autoCreationParamsNotAllow")
    public void testGetMetadataIfNotAllowedCreate(boolean configAllowAutoTopicCreation,
                                                  boolean paramMetadataAutoCreationEnabled,
                                                  boolean isUsingHttpLookup) throws Exception {
        super.testGetMetadataIfNotAllowedCreate(configAllowAutoTopicCreation, paramMetadataAutoCreationEnabled,
                isUsingHttpLookup);
    }

    /**
     * {@inheritDoc}
     */
    @Test(dataProvider = "autoCreationParamsNotAllow")
    public void testGetMetadataIfNotAllowedCreateOfNonPersistentTopic(boolean configAllowAutoTopicCreation,
                                                  boolean paramMetadataAutoCreationEnabled,
                                                  boolean isUsingHttpLookup) throws Exception {
        super.testGetMetadataIfNotAllowedCreateOfNonPersistentTopic(configAllowAutoTopicCreation,
                paramMetadataAutoCreationEnabled, isUsingHttpLookup);
    }

    @DataProvider(name = "autoCreationParamsAllForNonPersistentTopic")
    public Object[][] autoCreationParamsAllForNonPersistentTopic(){
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

    @Test(dataProvider = "autoCreationParamsAllForNonPersistentTopic", priority = Integer.MAX_VALUE)
    public void testCompatibilityDifferentBrokersForNonPersistentTopic(boolean configAllowAutoTopicCreation,
                                                  boolean paramMetadataAutoCreationEnabled,
                                                  boolean isUsingHttpLookup) throws Exception {
        modifyTopicAutoCreation(configAllowAutoTopicCreation, TopicType.PARTITIONED, 3);

        // Verify: the method "getPartitionsForTopic(topic, false, true)" will fallback
        //   to "getPartitionsForTopic(topic, true)" behavior.
        int lookupPermitsBefore = getLookupRequestPermits();

        // Initialize the connections of internal Pulsar Client.
        PulsarClientImpl client1 = (PulsarClientImpl) pulsar1.getClient();
        PulsarClientImpl client2 = (PulsarClientImpl) pulsar2.getClient();
        client1.getLookup(pulsar2.getBrokerServiceUrl()).getBroker(TopicName.get(DEFAULT_NS + "/tp1")).join();
        client2.getLookup(pulsar1.getBrokerServiceUrl()).getBroker(TopicName.get(DEFAULT_NS + "/tp1")).join();

        // Inject a not support flag into the connections initialized.
        Field field = ClientCnx.class.getDeclaredField("supportsGetPartitionedMetadataWithoutAutoCreation");
        field.setAccessible(true);
        for (PulsarClientImpl client : Arrays.asList(client1, client2)) {
            ConnectionPool pool = client.getCnxPool();
            for (CompletableFuture<ClientCnx> connectionFuture : pool.getConnectionByResolver()) {
                ClientCnx clientCnx = connectionFuture.join();
                clientCnx.isSupportsGetPartitionedMetadataWithoutAutoCreation();
                field.set(clientCnx, false);
            }
        }

        // Verify: we will not get an un-support error.
        PulsarClientImpl[] clientArray = getClientsToTest(isUsingHttpLookup);
        for (PulsarClientImpl client : clientArray) {
            final String topicNameStr = BrokerTestUtil.newUniqueName("non-persistent://" + DEFAULT_NS + "/tp");
            try {
                PartitionedTopicMetadata topicMetadata = client
                        .getPartitionedTopicMetadata(topicNameStr, paramMetadataAutoCreationEnabled, false)
                        .join();
                log.info("Get topic metadata: {}", topicMetadata.partitions);
            } catch (Exception ex) {
                Throwable unwrapEx = FutureUtil.unwrapCompletionException(ex);
                assertTrue(unwrapEx instanceof PulsarClientException.TopicDoesNotExistException
                        || unwrapEx instanceof PulsarClientException.NotFoundException);
                assertFalse(ex.getMessage().contains("getting partitions without auto-creation is not supported from"
                        + " the broker"));
            }
        }

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
        });

        // reset clients.
        for (PulsarClientImpl client : Arrays.asList(client1, client2)) {
            ConnectionPool pool = client.getCnxPool();
            for (CompletableFuture<ClientCnx> connectionFuture : pool.getConnectionByResolver()) {
                ClientCnx clientCnx = connectionFuture.join();
                clientCnx.isSupportsGetPartitionedMetadataWithoutAutoCreation();
                field.set(clientCnx, true);
            }
        }
    }
}
