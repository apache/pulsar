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

import io.opentelemetry.api.OpenTelemetry;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicFactory;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * A test interceptor for broker tests that allows to decorate persistent topics, subscriptions, dispatchers
 * managed ledger factory, managed ledger and managed cursor instances.
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

    public static class TestManagedLedgerStorage extends ManagedLedgerClientFactory {
        @Override
        protected ManagedLedgerFactoryImpl createManagedLedgerFactory(MetadataStoreExtended metadataStore,
                                                                      OpenTelemetry openTelemetry,
                                                                      ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory,
                                                                      ManagedLedgerFactoryConfig managedLedgerFactoryConfig,
                                                                      StatsLogger statsLogger) throws Exception {
            return INSTANCE.managedLedgerFactoryDecorator.apply(
                    new TestManagedLedgerFactoryImpl(metadataStore, bkFactory, managedLedgerFactoryConfig, statsLogger,
                            openTelemetry));
        }
    }

    static class TestManagedLedgerFactoryImpl extends ManagedLedgerFactoryImpl {
        public TestManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                            BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                            ManagedLedgerFactoryConfig config, StatsLogger statsLogger,
                                            OpenTelemetry openTelemetry) throws Exception {
            super(metadataStore, bookKeeperGroupFactory, config, statsLogger, openTelemetry);
        }

        @Override
        protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                                                        ManagedLedgerConfig config,
                                                        Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
            return INSTANCE.managedLedgerDecorator.apply(
                    new TestManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name, mlOwnershipChecker));
        }
    }

    static class TestManagedLedgerImpl extends ManagedLedgerImpl {
        public TestManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
                                     ManagedLedgerConfig config,
                                     OrderedScheduler scheduledExecutor, String name,
                                     Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
            super(factory, bookKeeper, store, config, scheduledExecutor, name, mlOwnershipChecker);
        }

        @Override
        protected ManagedCursorImpl createCursor(BookKeeper bookKeeper, String cursorName) {
            return INSTANCE.managedCursorDecorator.apply(super.createCursor(bookKeeper, cursorName));
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

    @Getter
    @Setter
    private Function<ManagedLedgerFactoryImpl, ManagedLedgerFactoryImpl> managedLedgerFactoryDecorator = Function.identity();

    @Getter
    @Setter
    private Function<ManagedLedgerImpl, ManagedLedgerImpl> managedLedgerDecorator = Function.identity();

    @Getter
    @Setter
    private Function<ManagedCursorImpl, ManagedCursorImpl> managedCursorDecorator = Function.identity();

    public void reset() {
        persistentTopicDecorator = Function.identity();
        persistentSubscriptionDecorator = Function.identity();
        dispatcherDecorator = Function.identity();
        managedLedgerFactoryDecorator = Function.identity();
        managedLedgerDecorator = Function.identity();
        managedCursorDecorator = Function.identity();
    }

    public void configure(ServiceConfiguration conf) {
        conf.setTopicFactoryClassName(TestTopicFactory.class.getName());
        conf.setManagedLedgerStorageClassName(TestManagedLedgerStorage.class.getName());
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

    public void applyCursorSpyDecorator(java.util.function.Consumer<ManagedCursorImpl> spyCustomizer) {
        setManagedCursorDecorator(cursor -> {
            ManagedCursorImpl spy = BrokerTestUtil.spyWithoutRecordingInvocations(cursor);
            spyCustomizer.accept(spy);
            return spy;
        });
    }
}
