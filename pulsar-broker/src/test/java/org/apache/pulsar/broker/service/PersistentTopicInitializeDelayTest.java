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
package org.apache.pulsar.broker.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "broker")
@Slf4j
public class PersistentTopicInitializeDelayTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setTopicFactoryClassName(MyTopicFactory.class.getName());
        conf.setAllowAutoTopicCreation(true);
        conf.setManagedLedgerMaxEntriesPerLedger(1);
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
        conf.setTransactionCoordinatorEnabled(false);
        conf.setTopicLoadTimeoutSeconds(30);
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30 * 1000)
    public void testTopicInitializeDelay() throws Exception {
        admin.tenants().createTenant("public", TenantInfo.builder().allowedClusters(Set.of(configClusterName)).build());
        String namespace = "public/initialize-delay";
        admin.namespaces().createNamespace(namespace);
        final String topicName = "persistent://" + namespace + "/testTopicInitializeDelay";
        admin.topics().createNonPartitionedTopic(topicName);

        admin.topicPolicies().setMaxConsumers(topicName, 10);
        Awaitility.await().untilAsserted(() -> assertEquals(admin.topicPolicies().getMaxConsumers(topicName), 10));
        admin.topics().unload(topicName);
        CompletableFuture<Optional<Topic>> optionalFuture = pulsar.getBrokerService().getTopic(topicName, true);

        Optional<Topic> topic = optionalFuture.get(15, TimeUnit.SECONDS);
        assertTrue(topic.isPresent());
    }

    public static class MyTopicFactory implements TopicFactory {
        @Override
        public <T extends Topic> T create(String topic, ManagedLedger ledger, BrokerService brokerService,
                                          Class<T> topicClazz) {
            try {
                if (topicClazz == NonPersistentTopic.class) {
                    return (T) new NonPersistentTopic(topic, brokerService);
                } else {
                    return (T) new MyPersistentTopic(topic, ledger, brokerService);
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void close() throws IOException {
            // No-op
        }
    }

    public static class MyPersistentTopic extends PersistentTopic {

        private static AtomicInteger checkReplicationInvocationCount = new AtomicInteger(0);

        public MyPersistentTopic(String topic, ManagedLedger ledger, BrokerService brokerService) {
            super(topic, ledger, brokerService);
            SystemTopicBasedTopicPoliciesService topicPoliciesService =
                    (SystemTopicBasedTopicPoliciesService) brokerService.getPulsar().getTopicPoliciesService();
            if (topicPoliciesService.getListeners().containsKey(TopicName.get(topic)) ) {
                brokerService.getPulsar().getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(topic),
                        TopicPoliciesService.GetType.DEFAULT
                ).thenAccept(optionalPolicies -> optionalPolicies.ifPresent(this::onUpdate));
            }
        }

        protected void updateTopicPolicyByNamespacePolicy(Policies namespacePolicies) {
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            super.updateTopicPolicyByNamespacePolicy(namespacePolicies);
        }

        public CompletableFuture<Void> checkReplication() {
            if (TopicName.get(topic).getLocalName().equalsIgnoreCase("testTopicInitializeDelay")) {
                checkReplicationInvocationCount.incrementAndGet();
                log.info("checkReplication, count = {}", checkReplicationInvocationCount.get());
                List<String> configuredClusters = topicPolicies.getReplicationClusters().get();
                if (!(configuredClusters.size() == 1 && configuredClusters.contains(brokerService.pulsar().getConfiguration().getClusterName()))) {
                    try {
                        // this will cause the get topic timeout.
                        Thread.sleep(8 * 1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException("checkReplication error");
                }
            }
            return super.checkReplication();
        }
    }
}
