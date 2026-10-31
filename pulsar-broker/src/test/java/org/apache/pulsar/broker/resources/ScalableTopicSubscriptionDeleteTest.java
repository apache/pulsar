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
package org.apache.pulsar.broker.resources;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

/**
 * Deleting a subscription of a scalable topic ({@link ScalableTopicResources#deleteSubscriptionAsync}) on ZooKeeper
 * and Oxia, which refuse to delete a record that has children: the consumer registrations of a subscription are
 * under a {@code consumers} record, a child of the subscription's record.
 */
public class ScalableTopicSubscriptionDeleteTest extends BaseMetadataStoreTest {

    private TopicName newTopic() {
        return TopicName.get("topic://tenant/ns" + newKey());
    }

    @Test(dataProvider = "distributedImpl")
    public void testDeleteSubscriptionWithRegisteredConsumers(String provider, Supplier<String> urlSupplier)
            throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);
        TopicName topic = newTopic();
        resources.createSubscriptionAsync(topic, "sub", SubscriptionType.STREAM).get();
        resources.registerConsumerAsync(topic, "sub", "c1").get();
        resources.registerConsumerAsync(topic, "sub", "c2").get();

        resources.deleteSubscriptionAsync(topic, "sub").get();

        assertThat(resources.subscriptionExistsAsync(topic, "sub").get()).isFalse();
        assertThat(resources.listSubscriptionsAsync(topic).get()).isEmpty();
        // A subscription created again with the same name has none of the consumers of the deleted one
        resources.createSubscriptionAsync(topic, "sub", SubscriptionType.STREAM).get();
        assertThat(resources.listConsumersAsync(topic, "sub").get()).isEmpty();
    }

    /** The controller unregisters the consumers of a subscription, then deletes the subscription. */
    @Test(dataProvider = "distributedImpl")
    public void testDeleteSubscriptionWhoseConsumersUnregistered(String provider, Supplier<String> urlSupplier)
            throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);
        TopicName topic = newTopic();
        resources.createSubscriptionAsync(topic, "sub", SubscriptionType.STREAM).get();
        resources.registerConsumerAsync(topic, "sub", "c1").get();
        resources.unregisterConsumerAsync(topic, "sub", "c1").get();

        resources.deleteSubscriptionAsync(topic, "sub").get();

        assertThat(resources.subscriptionExistsAsync(topic, "sub").get()).isFalse();
        assertThat(resources.listSubscriptionsAsync(topic).get()).isEmpty();
        // Deleting a subscription that is already gone succeeds
        resources.deleteSubscriptionAsync(topic, "sub").get();
    }
}
