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

import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import com.google.common.collect.Sets;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class CompactionReaderImplTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void test() throws Exception {

        String topic = "persistent://my-property/my-ns/my-compact-topic";

        // subscribe before sending anything, so that we get all messages
        @Cleanup
        var consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe();
        int numKeys = 5;
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        for (int i = 0; i < numKeys; i++) {
            producer.newMessage().key("key:" + i).value("value" + i).send();
        }

        @Cleanup
        CompactionReaderImpl<String> reader = CompactionReaderImpl
                .create((PulsarClientImpl) pulsarClient, Schema.STRING, topic, new CompletableFuture(), null);

        ConsumerBase consumerBase = spy(reader.getConsumer());
        FieldUtils.writeDeclaredField(
                reader, "consumer", consumerBase, true);

        ReaderConfigurationData readerConfigurationData =
                (ReaderConfigurationData) FieldUtils.readDeclaredField(
                        reader, "readerConfiguration", true);


        ReaderConfigurationData expected = new ReaderConfigurationData<>();
        expected.setTopicName(topic);
        expected.setSubscriptionName(COMPACTION_SUBSCRIPTION);
        expected.setStartMessageId(MessageId.earliest);
        expected.setStartMessageFromRollbackDurationInSec(0);
        expected.setReadCompacted(true);
        expected.setSubscriptionMode(SubscriptionMode.Durable);
        expected.setSubscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

        MessageIdImpl lastMessageId = (MessageIdImpl) reader.getLastMessageIdAsync().get();
        MessageIdImpl id = null;
        MessageImpl m = null;

        Assert.assertEquals(readerConfigurationData, expected);
        for (int i = 0; i < numKeys; i++) {
            m = (MessageImpl) reader.readNextAsync().get();
            id = (MessageIdImpl) m.getMessageId();
        }
        Assert.assertEquals(id, lastMessageId);
        verify(consumerBase, times(0))
                .acknowledgeCumulativeAsync(Mockito.any(MessageId.class));

    }
}
