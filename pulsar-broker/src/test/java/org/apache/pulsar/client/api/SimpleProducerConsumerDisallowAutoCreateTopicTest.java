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
package org.apache.pulsar.client.api;

import static org.apache.pulsar.client.util.RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class SimpleProducerConsumerDisallowAutoCreateTopicTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setAllowAutoTopicCreation(false);
    }

    @Test
    public void testClearErrorIfRetryTopicNotExists() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        final String subName = "sub";
        final String retryTopicName = topicName + "-" + subName + RETRY_GROUP_TOPIC_SUFFIX;
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
            log.info("got an expected error", ex);
            assertTrue(ex.getMessage().contains("Not found:"));
            assertTrue(ex.getMessage().contains(retryTopicName));
        } finally {
            // cleanup.
            if (consumer != null) {
                consumer.close();
            }
            admin.topics().delete(topicName);
        }
    }
}
