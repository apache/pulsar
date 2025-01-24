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
package org.apache.pulsar.broker.testinterceptor;

import java.util.Map;
import java.util.function.Function;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicFactory;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * A test interceptor for broker tests that allows to decorate persistent topics, subscriptions and dispatchers.
 */
public class BrokerTestInterceptor {
    public static final BrokerTestInterceptor INSTANCE = new BrokerTestInterceptor();

    // Suppress default constructor for noninstantiability
    private BrokerTestInterceptor() {

    }

    public static class TestTopicFactory implements TopicFactory {
        @Override
        public <T extends Topic> T create(String topic, ManagedLedger ledger, BrokerService brokerService,
                                          Class<T> topicClazz) {
            if (!topicClazz.isAssignableFrom(PersistentTopic.class)) {
                throw new UnsupportedOperationException("Unsupported topic class");
            }
            return topicClazz.cast(
                    INSTANCE.getPersistentTopicDecorator().apply(new TestTopic(topic, ledger, brokerService)));
        }
    }

    static class TestTopic extends PersistentTopic {

        public TestTopic(String topic, ManagedLedger ledger, BrokerService brokerService) {
            super(topic, ledger, brokerService);
        }

        @Override
        protected PersistentSubscription createPersistentSubscription(String subscriptionName, ManagedCursor cursor,
                                                                      Boolean replicated,
                                                                      Map<String, String> subscriptionProperties) {
            return INSTANCE.getPersistentSubscriptionDecorator()
                    .apply(new TestSubscription(this, subscriptionName, cursor, replicated, subscriptionProperties));
        }
    }

    static class TestSubscription extends PersistentSubscription {
        public TestSubscription(PersistentTopic topic, String subscriptionName, ManagedCursor cursor,
                                Boolean replicated,
                                Map<String, String> subscriptionProperties) {
            super(topic, subscriptionName, cursor, replicated, subscriptionProperties);
        }

        @Override
        protected Dispatcher reuseOrCreateDispatcher(Dispatcher dispatcher,
                                                     Consumer consumer) {
            Dispatcher previousInstance = dispatcher;
            dispatcher = super.reuseOrCreateDispatcher(dispatcher, consumer);
            if (dispatcher != previousInstance) {
                dispatcher = INSTANCE.getDispatcherDecorator().apply(dispatcher);
            }
            return dispatcher;
        }
    }

    @Getter
    @Setter
    private Function<PersistentTopic, PersistentTopic> persistentTopicDecorator = Function.identity();

    @Getter
    @Setter
    private Function<PersistentSubscription, PersistentSubscription> persistentSubscriptionDecorator = Function.identity();

    @Getter
    @Setter
    private Function<Dispatcher, Dispatcher> dispatcherDecorator = Function.identity();

    public void reset() {
        persistentTopicDecorator = Function.identity();
        persistentSubscriptionDecorator = Function.identity();
        dispatcherDecorator = Function.identity();
    }

    public void configure(ServiceConfiguration conf) {
        conf.setTopicFactoryClassName(TestTopicFactory.class.getName());
    }

    public  <T extends Dispatcher> void applyDispatcherSpyDecorator(Class<T> dispatcherClass,
                                                                    java.util.function.Consumer<T> spyCustomizer) {
        setDispatcherDecorator(createDispatcherSpyDecorator(dispatcherClass, spyCustomizer));
    }

    public static <T extends Dispatcher> Function<Dispatcher, Dispatcher> createDispatcherSpyDecorator(
            Class<T> dispatcherClass, java.util.function.Consumer<T> spyCustomizer) {
        return dispatcher -> {
            Dispatcher spy = BrokerTestUtil.spyWithoutRecordingInvocations(dispatcher);
            spyCustomizer.accept(dispatcherClass.cast(spy));
            return spy;
        };
    }
}
