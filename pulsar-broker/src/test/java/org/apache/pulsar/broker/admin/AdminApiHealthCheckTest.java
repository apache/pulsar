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
package org.apache.pulsar.broker.admin;

import static org.apache.pulsar.broker.admin.impl.BrokersBase.HEALTH_CHECK_TOPIC_SUFFIX;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.compaction.Compactor;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.springframework.util.CollectionUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
public class AdminApiHealthCheckTest extends MockedPulsarServiceBaseTest {

    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(
                Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", Set.of("test"));
        admin.tenants().createTenant("public", tenantInfo);
        admin.namespaces().createNamespace("public/default", Set.of("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "topicVersion")
    public static Object[][] topicVersions() {
        return new Object[][] {
                { null },
                { TopicVersion.V1 },
                { TopicVersion.V2 },
        };
    }

    @Test(dataProvider = "topicVersion")
    public void testHealthCheckup(TopicVersion topicVersion) throws Exception {
        final int times = 30;
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar.getExecutor().execute(() -> {
            try {
                for (int i = 0; i < times; i++) {
                    if (topicVersion == null) {
                        admin.brokers().healthcheck();
                    } else {
                        admin.brokers().healthcheck(topicVersion);
                    }
                }
                future.complete(null);
            }catch (PulsarAdminException e) {
                future.completeExceptionally(e);
            }
        });
        for (int i = 0; i < times; i++) {
            if (topicVersion == null) {
                admin.brokers().healthcheck();
            } else {
                admin.brokers().healthcheck(topicVersion);
            }
        }
        // To ensure we don't have any subscription
        String brokerId = pulsar.getBrokerId();
        NamespaceName namespaceName = (topicVersion == TopicVersion.V2)
                ? NamespaceService.getHeartbeatNamespaceV2(brokerId, pulsar.getConfiguration())
                : NamespaceService.getHeartbeatNamespace(brokerId, pulsar.getConfiguration());
        final String testHealthCheckTopic = String.format("persistent://%s/%s", namespaceName, HEALTH_CHECK_TOPIC_SUFFIX);
        Awaitility.await().untilAsserted(() -> {
            assertFalse(future.isCompletedExceptionally());
        });
        Awaitility.await().untilAsserted(() ->
                assertTrue(CollectionUtils.isEmpty(admin.topics()
                        .getSubscriptions(testHealthCheckTopic).stream()
                        // All system topics are using compaction, even though is not explicitly set in the policies.
                        .filter(v -> !v.equals(Compactor.COMPACTION_SUBSCRIPTION))
                        .collect(Collectors.toList())
                ))
        );
    }

    @Test(expectedExceptions= PulsarAdminException.class, expectedExceptionsMessageRegExp = ".*Deadlocked threads detected.*")
    public void testHealthCheckupDetectsDeadlock() throws Exception {
        // simulate a deadlock in the Test JVM
        // the broker used in unit tests runs in the test JVM and the
        // healthcheck implementation should detect this deadlock
        Lock lock1 = new ReentrantReadWriteLock().writeLock();
        Lock lock2 = new ReentrantReadWriteLock().writeLock();
        final Phaser phaser = new Phaser(3);
        Thread thread1=new Thread(() -> {
            phaser.arriveAndAwaitAdvance();
            try {
                deadlock(lock1, lock2, 1000L);
            } finally {
                phaser.arriveAndDeregister();
            }
        }, "deadlockthread-1");
        Thread thread2=new Thread(() -> {
            phaser.arriveAndAwaitAdvance();
            try {
                deadlock(lock2, lock1, 2000L);
            } finally {
                phaser.arriveAndDeregister();
            }
        }, "deadlockthread-2");
        thread1.start();
        thread2.start();
        phaser.arriveAndAwaitAdvance();
        Thread.sleep(5000L);

        try {
            admin.brokers().healthcheck(TopicVersion.V2);
        } finally {
            // unlock the deadlock
            thread1.interrupt();
            thread2.interrupt();
            // wait for deadlock threads to finish
            phaser.arriveAndAwaitAdvance();
            // wait for deadlocked status to clear before continuing
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> threadBean.findDeadlockedThreads() == null);
        }
    }

    private void deadlock(Lock lock1, Lock lock2, long millis) {
        try {
            lock1.lockInterruptibly();
            try {
                Thread.sleep(millis);
                lock2.lockInterruptibly();
                lock2.unlock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock1.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test(timeOut = 5000L)
    public void testDeadlockDetectionOverhead() {
        for (int i=0; i < 1000; i++) {
            long[] threadIds = threadBean.findDeadlockedThreads();
            // assert that there's no deadlock
            assertNull(threadIds);
        }
    }

    class DummyProducerBuilder<T> extends ProducerBuilderImpl<T> {
        // This is a dummy producer builder to test the health check timeout
        // the producer constructed by this builder will not send any message
        public DummyProducerBuilder(PulsarClientImpl client, Schema schema) {
            super(client, schema);
        }

        @Override
        public CompletableFuture<Producer<T>> createAsync() {
            CompletableFuture<Producer<T>> future = new CompletableFuture<>();
            super.createAsync().thenAccept(producer -> {
                Producer<T> spyProducer = Mockito.spy(producer);
                Mockito.doReturn(CompletableFuture.completedFuture(MessageId.earliest))
                        .when(spyProducer).sendAsync(Mockito.any());
                future.complete(spyProducer);
            }).exceptionally(ex -> {
                future.completeExceptionally(ex);
                return null;
            });
            return future;
        }
    }

    @Test
    public void testHealthCheckTimeOut() throws Exception {
        final String testHealthCheckTopic = String.format("persistent://pulsar/localhost:%s/healthcheck",
                pulsar.getConfig().getWebServicePort().get());
        PulsarClient client = pulsar.getClient();
        PulsarClient spyClient = Mockito.spy(client);
        Mockito.doReturn(new DummyProducerBuilder<>((PulsarClientImpl) spyClient, Schema.BYTES))
                .when(spyClient).newProducer(Schema.STRING);
        // use reflection to replace the client in the broker
        Field field = PulsarService.class.getDeclaredField("client");
        field.setAccessible(true);
        field.set(pulsar, spyClient);
        try {
            admin.brokers().healthcheck(TopicVersion.V2);
            throw new Exception("Should not reach here");
        } catch (PulsarAdminException e) {
            log.info("Exception caught", e);
            assertTrue(e.getMessage().contains("LowOverheadTimeoutException"));
        }
        // To ensure we don't have any subscription, the producers and readers are closed.
        Awaitility.await().untilAsserted(() ->
                assertTrue(CollectionUtils.isEmpty(admin.topics()
                        .getSubscriptions(testHealthCheckTopic).stream()
                        // All system topics are using compaction, even though is not explicitly set in the policies.
                        .filter(v -> !v.equals(Compactor.COMPACTION_SUBSCRIPTION))
                        .collect(Collectors.toList())
                ))
        );
    }

}
