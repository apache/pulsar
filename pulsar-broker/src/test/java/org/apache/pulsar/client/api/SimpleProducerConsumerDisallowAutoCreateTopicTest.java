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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.PulsarClientException.TopicDoesNotExistException;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-api")
public class SimpleProducerConsumerDisallowAutoCreateTopicTest extends SharedPulsarBaseTest {

    @BeforeMethod(alwaysRun = true)
    public void setupDisallowAutoCreate() throws Exception {
        admin.namespaces().setAutoTopicCreation(getNamespace(),
                AutoTopicCreationOverride.builder().allowAutoTopicCreation(false).build());
    }

    @Test
    public void testClearErrorIfRetryTopicNotExists() throws Exception {
        final String topicName = newTopicName();
        final String subName = "sub";
        admin.topics().createNonPartitionedTopic(topicName);
        Consumer consumer = null;
        try {
            consumer = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subName)
                    .enableRetry(true)
                    .subscribe();
            fail("");
        } catch (Exception ex) {
            log.info().exception(ex).log("got an expected error");
            assertTrue(ex instanceof TopicDoesNotExistException,
                    "Expected TopicDoesNotExistException but got: " + ex.getClass().getName());
        } finally {
            // cleanup.
            if (consumer != null) {
                consumer.close();
            }
        }
    }
}
