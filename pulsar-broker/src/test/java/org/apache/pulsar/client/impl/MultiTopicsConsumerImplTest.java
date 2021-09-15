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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author hezhangjian
 */
@Test(groups = "broker-impl")
public class MultiTopicsConsumerImplTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20_000)
    public void testReceiveAfterClose() throws Exception {
        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).build();
        String topic = "persistent://public/default/receive-after-close1";
        String topic2 = "persistent://public/default/receive-after-close2";
        admin.topics().createPartitionedTopic(topic, 3);
        admin.topics().createNonPartitionedTopic(topic2);
        MultiTopicsConsumerImpl<byte[]> multiConsumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1").subscribe();
        final MultiTopicsConsumerImpl<byte[]> multiTopicsConsumer = Mockito.spy(multiConsumer);
        final ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic2).subscriptionName("sub-1").subscribe();
        consumer.close();
        Mockito.doCallRealMethod().when(multiTopicsConsumer).receiveMessageFromConsumer(Mockito.notNull());
        multiTopicsConsumer.receiveMessageFromConsumer(consumer);
        Thread.sleep((MultiTopicsConsumerImpl.RECEIVE_ASYNC_RETRY_INTERVAL_SECONDS + 3) * 1000);
        Mockito.verify(multiTopicsConsumer, Mockito.times(1)).receiveMessageFromConsumer(Mockito.notNull());
    }

}
