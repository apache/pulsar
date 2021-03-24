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
package org.apache.pulsar.broker.service;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.assertj.core.util.Sets;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for the broker entry metadata.
 */
@Test(groups = "broker")
public class BrokerEntryMetadataE2ETest extends BrokerTestBase {

    @DataProvider(name = "subscriptionTypes")
    public static Object[] subscriptionTypes() {
        return new Object[] {
                SubscriptionType.Exclusive,
                SubscriptionType.Failover,
                SubscriptionType.Shared,
                SubscriptionType.Key_Shared
        };
    }

    @BeforeClass
    protected void setup() throws Exception {
        conf.setBrokerEntryMetadataInterceptors(Sets.newTreeSet(
                "org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor",
                "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor"
                ));
        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testProduceAndConsume(SubscriptionType subType) throws Exception {
        final String topic = newTopicName();
        final int messages = 10;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(subType)
                .subscriptionName("my-sub")
                .subscribe();

        for (int i = 0; i < messages; i++) {
            producer.send(String.valueOf(i).getBytes());
        }

        int receives = 0;
        for (int i = 0; i < messages; i++) {
            Message<byte[]> received = consumer.receive();
            ++ receives;
            Assert.assertEquals(i, Integer.valueOf(new String(received.getValue())).intValue());
        }

        Assert.assertEquals(messages, receives);
    }
}
