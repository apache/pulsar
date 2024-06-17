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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.net.URL;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
}
