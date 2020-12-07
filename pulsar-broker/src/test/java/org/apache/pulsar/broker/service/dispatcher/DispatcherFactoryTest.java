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
package org.apache.pulsar.broker.service.dispatcher;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.PersistentDispatcherFailoverConsumerTest;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@PrepareForTest({
        PulsarApi.KeySharedMeta.class, DispatcherFactory.class
})
@PowerMockIgnore({"org.apache.logging.log4j.*"})
public class DispatcherFactoryTest {

    @Test
    public void testCanGetDefaultDispatchers() throws BrokerServiceException {
        ServiceConfiguration config = new ServiceConfiguration();
        ManagedCursor cursor = mock(ManagedCursor.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        when(pulsarService.getConfiguration()).thenReturn(config);
        when(brokerService.pulsar()).thenReturn(pulsarService);
        when(brokerService.getPulsar()).thenReturn(pulsarService);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(topic.getName()).thenReturn("my-topic");
        when(cursor.getName()).thenReturn("my-cursor");
        Subscription subscription = mock(PersistentSubscription.class);
        PulsarApi.KeySharedMeta ksm = PulsarApi.KeySharedMeta.getDefaultInstance();

        DispatcherConfiguration dispatcherConfiguration = new DispatcherConfiguration(PulsarApi.CommandSubscribe.SubType.Exclusive,
                cursor, 0, topic, subscription, ksm);
        Dispatcher dispatcher = DispatcherFactory.getDispatcher(dispatcherConfiguration, config);
        assertTrue(dispatcher instanceof PersistentDispatcherSingleActiveConsumer);
        assertEquals(dispatcher.getType(), PulsarApi.CommandSubscribe.SubType.Exclusive);

        dispatcherConfiguration.setSubType(PulsarApi.CommandSubscribe.SubType.Failover);
        dispatcher = DispatcherFactory.getDispatcher(dispatcherConfiguration, config);
        System.out.println(dispatcher.getClass());
        assertTrue(dispatcher instanceof PersistentDispatcherSingleActiveConsumer);
        assertEquals(dispatcher.getType(), PulsarApi.CommandSubscribe.SubType.Failover);

        dispatcherConfiguration.setSubType(PulsarApi.CommandSubscribe.SubType.Shared);
        dispatcher = DispatcherFactory.getDispatcher(dispatcherConfiguration, config);
        assertTrue(dispatcher instanceof PersistentDispatcherMultipleConsumers);

        dispatcherConfiguration.setSubType(PulsarApi.CommandSubscribe.SubType.Key_Shared);
        dispatcher = DispatcherFactory.getDispatcher(dispatcherConfiguration, config);
        assertTrue(dispatcher instanceof PersistentStickyKeyDispatcherMultipleConsumers);

        NonPersistentTopic nonpersistentTopic = mock(NonPersistentTopic.class);
        Subscription nonPersistentSubscription = mock(NonPersistentSubscription.class);
        when(nonpersistentTopic.getBrokerService()).thenReturn(brokerService);
        when(nonpersistentTopic.getName()).thenReturn("my-topic");
        dispatcherConfiguration.setTopic(nonpersistentTopic);
        dispatcherConfiguration.setSubscription(nonPersistentSubscription);
        dispatcherConfiguration.setSubType(PulsarApi.CommandSubscribe.SubType.Exclusive);

        dispatcher = DispatcherFactory.getDispatcher(dispatcherConfiguration, config);
        assertTrue(dispatcher instanceof NonPersistentDispatcherSingleActiveConsumer);
        assertEquals(dispatcher.getType(), PulsarApi.CommandSubscribe.SubType.Exclusive);

        dispatcherConfiguration.setSubType(PulsarApi.CommandSubscribe.SubType.Failover);
        dispatcher = DispatcherFactory.getDispatcher(dispatcherConfiguration, config);
        assertTrue(dispatcher instanceof NonPersistentDispatcherSingleActiveConsumer);
        assertEquals(dispatcher.getType(), PulsarApi.CommandSubscribe.SubType.Failover);

        dispatcherConfiguration.setSubType(PulsarApi.CommandSubscribe.SubType.Shared);
        dispatcher = DispatcherFactory.getDispatcher(dispatcherConfiguration, config);
        assertTrue(dispatcher instanceof NonPersistentDispatcherMultipleConsumers);

        dispatcherConfiguration.setSubType(PulsarApi.CommandSubscribe.SubType.Key_Shared);
        dispatcher = DispatcherFactory.getDispatcher(dispatcherConfiguration, config);
        assertTrue(dispatcher instanceof NonPersistentStickyKeyDispatcherMultipleConsumers);
    }
}
