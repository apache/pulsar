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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class IgnoreConsumerReplicateSubscriptionStateTest extends MockedPulsarServiceBaseTest {
    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        Properties properties = conf.getProperties();
        if (properties==null) {
            properties = new Properties();
        }
        properties.put("ignoreConsumerReplicateSubscriptionState", "true");
    }

    @DataProvider
    Object[] replicateSubscriptionState(){
        return new Object[]{true,false};
    }

    @Test(dataProvider = "replicateSubscriptionState")
    public void ignoreConsumerReplicateSubscriptionState(boolean enabled)
            throws PulsarClientException, ExecutionException, InterruptedException {
        String topicName = "topic-" + System.nanoTime();
        String subName = "sub-" + System.nanoTime();
        @Cleanup Consumer<byte[]> ignored =
                pulsarClient.newConsumer().topic(topicName).replicateSubscriptionState(enabled)
                        .subscriptionName(subName).subscribe();
        Optional<Topic> topicOptional = pulsar.getBrokerService().getTopic(topicName, false).get();
        assertTrue(topicOptional.isPresent());
        PersistentTopic persistentTopic = (PersistentTopic) topicOptional.get();
        PersistentSubscription persistentSubscription = persistentTopic.getSubscription(subName);
        assertNotNull(persistentSubscription);
        assertNull(persistentSubscription.getReplicatedControlled());
    }
}