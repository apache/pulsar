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
package org.apache.pulsar.broker.loadbalance;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Replaces the broker's load manager through the {@code loadManagerClassName} dynamic configuration. Each method gets
 * a broker of its own, since it replaces the broker's load manager.
 */
@Test(groups = "broker")
public class LoadManagerDynamicSwitchTest extends MockedPulsarServiceBaseTest {

    private static final String LOAD_MANAGER_CLASS_NAME = "loadManagerClassName";

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public Object[][] otherLoadManagers() {
        return new Object[][]{{SimpleLoadManagerImpl.class}, {NoopLoadManager.class}};
    }

    @Test(dataProvider = "otherLoadManagers")
    public void testSwitchAndBack(Class<? extends LoadManager> otherLoadManager) throws Exception {
        LoadManager original = pulsar.getLoadManager().get();

        admin.brokers().updateDynamicConfiguration(LOAD_MANAGER_CLASS_NAME, otherLoadManager.getName());
        Awaitility.await().until(() -> otherLoadManager.isInstance(pulsar.getLoadManager().get()));
        admin.brokers().deleteDynamicConfiguration(LOAD_MANAGER_CLASS_NAME);
        awaitNewModularLoadManager(original);

        assertTopicLookupSucceeds();
    }

    @Test(dataProvider = "otherLoadManagers")
    public void testSwitchAndBackWithoutWaiting(Class<? extends LoadManager> otherLoadManager) throws Exception {
        LoadManager original = pulsar.getLoadManager().get();

        admin.brokers().updateDynamicConfiguration(LOAD_MANAGER_CLASS_NAME, otherLoadManager.getName());
        // Change it back as soon as the broker took the change, while it replaces its load manager
        Awaitility.await().until(() -> isConfiguredLoadManager(otherLoadManager));
        admin.brokers().deleteDynamicConfiguration(LOAD_MANAGER_CLASS_NAME);
        awaitNewModularLoadManager(original);

        assertTopicLookupSucceeds();
    }

    @Test
    public void testChangeWhileReplacingLoadManager() throws Exception {
        LoadManager original = pulsar.getLoadManager().get();
        BlockingStartLoadManager.starting = new CompletableFuture<>();
        BlockingStartLoadManager.canStart = new CompletableFuture<>();
        try {
            admin.brokers().updateDynamicConfiguration(LOAD_MANAGER_CLASS_NAME,
                    BlockingStartLoadManager.class.getName());
            // The broker stopped its load manager, and the new one waits in start()
            BlockingStartLoadManager blocked = BlockingStartLoadManager.starting.get(10, TimeUnit.SECONDS);

            admin.brokers().deleteDynamicConfiguration(LOAD_MANAGER_CLASS_NAME);
            Awaitility.await().until(() -> isConfiguredLoadManager(ModularLoadManagerImpl.class));
            Awaitility.await().alias("the replacement for the second change waits for the first one")
                    .during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                    .until(() -> pulsar.getLoadManager().get() == original);

            BlockingStartLoadManager.canStart.complete(null);
            // It then replaces the load manager that the first replacement started
            awaitNewModularLoadManager(original);
            assertThat(blocked.isStopped()).as("the load manager of the first replacement is stopped").isTrue();
        } finally {
            BlockingStartLoadManager.canStart.complete(null);
        }

        assertTopicLookupSucceeds();
    }

    private boolean isConfiguredLoadManager(Class<?> loadManagerClass) {
        return loadManagerClass.getName().equals(pulsar.getConfiguration().getLoadManagerClassName());
    }

    // Without the dynamic configuration, the broker uses the default load manager, ModularLoadManagerImpl
    private void awaitNewModularLoadManager(LoadManager original) {
        Awaitility.await().until(() -> pulsar.getLoadManager().get() instanceof ModularLoadManagerWrapper current
                && current != original);
    }

    private void assertTopicLookupSucceeds() throws Exception {
        // The broker has to assign the bundle of a new namespace, through its load manager
        String namespace = BrokerTestUtil.newUniqueName("public/ns");
        admin.namespaces().createNamespace(namespace);
        assertThat(admin.lookups().lookupTopic("persistent://" + namespace + "/topic"))
                .isEqualTo(pulsar.getBrokerServiceUrl());
    }

    /**
     * A {@link NoopLoadManager} whose {@link #start()} waits until the test lets it start, to hold the broker in the
     * middle of replacing its load manager. The broker creates it from its class name, so the test reaches it
     * through static fields.
     */
    public static class BlockingStartLoadManager extends NoopLoadManager {
        static volatile CompletableFuture<BlockingStartLoadManager> starting = new CompletableFuture<>();
        static volatile CompletableFuture<Void> canStart = new CompletableFuture<>();

        private volatile boolean stopped;

        @Override
        public void start() throws PulsarServerException {
            starting.complete(this);
            try {
                canStart.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PulsarServerException(e);
            } catch (ExecutionException e) {
                throw new PulsarServerException(e);
            }
            super.start();
        }

        @Override
        public void stop() throws PulsarServerException {
            stopped = true;
            super.stop();
        }

        boolean isStopped() {
            return stopped;
        }
    }
}
