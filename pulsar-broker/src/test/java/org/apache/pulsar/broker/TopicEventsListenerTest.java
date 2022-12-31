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
package org.apache.pulsar.broker;

import com.google.common.collect.Sets;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

@Slf4j
public class TopicEventsListenerTest extends BrokerTestBase {

    final static Queue<String> events = new ConcurrentLinkedQueue<>();
    String topicNameToWatch;
    String namespace;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

        pulsar.getBrokerService().addTopicEventListener((topic, event, stage, t) -> {
            log.info("got event {}__{} for topic {}", event, stage, topic);
            if (topic.equals(topicNameToWatch)) {
                if (log.isDebugEnabled()) {
                    log.debug("got event {}__{} for topic {} with detailed stack",
                            event, stage, topic, new Exception("tracing event source"));
                }
                events.add(event.toString() + "__" + stage.toString());
            }
        });

        namespace = "prop/" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        assertTrue(admin.namespaces().getNamespaces("prop").contains(namespace));
        admin.namespaces().setRetention(namespace, new RetentionPolicies(3, 10));

        events.clear();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        deleteNamespaceWithRetry(namespace, true);

        super.internalCleanup();
    }

    @Test
    public void testEventsNonPersistentNonPartitionedTopic() throws Exception {
        topicNameToWatch = "non-persistent://" + namespace + "/NP-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
            Assert.assertEquals(events.toArray(), new String[]{
                    "LOAD__BEFORE",
                    "LOAD__FAILURE",
                    "LOAD__BEFORE",
                    "CREATE__BEFORE",
                    "CREATE__SUCCESS",
                    "LOAD__SUCCESS"
            })
        );

        events.clear();
        admin.topics().delete(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsNonPersistentNonPartitionedTopicWithUnload() throws Exception {
        topicNameToWatch = "non-persistent://" + namespace + "/NP-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "LOAD__FAILURE",
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().unload(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().delete(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsNonPersistentNonPartitionedTopicForce() throws Exception {
        topicNameToWatch = "non-persistent://" + namespace + "/NP-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "LOAD__FAILURE",
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().delete(topicNameToWatch, true);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"
                })
        );

    }

    @Test
    public void testEventsNonPersistentNonPartitionedTopicWithUnloadForce() throws Exception {
        topicNameToWatch = "non-persistent://" + namespace + "/NP-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "LOAD__FAILURE",
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().unload(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().delete(topicNameToWatch, true);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsPersistentNonPartitionedTopic() throws Exception {
        topicNameToWatch = "persistent://" + namespace + "/P-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "LOAD__FAILURE",
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().delete(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsPersistentNonPartitionedTopicWithUnload() throws Exception {
        topicNameToWatch = "persistent://" + namespace + "/P-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "LOAD__FAILURE",
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().unload(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().delete(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsPersistentNonPartitionedTopicForce() throws Exception {
        topicNameToWatch = "persistent://" + namespace + "/P-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "LOAD__FAILURE",
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().delete(topicNameToWatch, true);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsPersistentNonPartitionedTopicWithUnloadForce() throws Exception {
        topicNameToWatch = "persistent://" + namespace + "/P-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "LOAD__FAILURE",
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().unload(topicNameToWatch);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().delete(topicNameToWatch, true);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "DELETE__SUCCESS"
                })
        );
    }


//---------

    @Test
    public void testEventsNonPersistentPartitionedTopic() throws Exception {
        String topicName = "non-persistent://" + namespace + "/P-P";
        topicNameToWatch = topicName + "-partition-1";
        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS",
                })
        );

        events.clear();

        admin.topics().deletePartitionedTopic(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsNonPersistentPartitionedTopicWithUnload() throws Exception {
        String topicName = "non-persistent://" + namespace + "/P-P";
        topicNameToWatch = topicName + "-partition-1";

        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().unload(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().deletePartitionedTopic(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsNonPersistentPartitionedTopicForce() throws Exception {
        String topicName = "non-persistent://" + namespace + "/P-NP";
        topicNameToWatch = topicName + "-partition-1";
        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().deletePartitionedTopic(topicName, true);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsNonPersistentPartitionedTopicWithUnloadForce() throws Exception {
        String topicName = "non-persistent://" + namespace + "/P-NP";
        topicNameToWatch = topicName + "-partition-1";
        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().unload(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().deletePartitionedTopic(topicName, true);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsPersistentPartitionedTopic() throws Exception {
        String topicName = "persistent://" + namespace + "/P-NP";
        topicNameToWatch = topicName + "-partition-1";
        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();

        admin.topics().deletePartitionedTopic(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsPersistentPartitionedTopicWithUnload() throws Exception {
        String topicName = "persistent://" + namespace + "/P-NP";
        topicNameToWatch = topicName + "-partition-0";
        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().unload(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().deletePartitionedTopic(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsPersistentPartitionedTopicForce() throws Exception {
        String topicName = "persistent://" + namespace + "/P-NP";
        topicNameToWatch = topicName + "-partition-1";

        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().deletePartitionedTopic(topicName, true);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testEventsPersistentPartitionedTopicWithUnloadForce() throws Exception {
        String topicName = "persistent://" + namespace + "/P-NP";
        topicNameToWatch = topicName + "-partition-0";

        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "LOAD__BEFORE",
                        "CREATE__BEFORE",
                        "CREATE__SUCCESS",
                        "LOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().unload(topicName);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS"
                })
        );

        events.clear();
        admin.topics().deletePartitionedTopic(topicName, true);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "DELETE__BEFORE",
                        "DELETE__SUCCESS"
                })
        );
    }

    @Test
    public void testNonPartitionedTopicAutoGC() throws Exception {
        topicNameToWatch = "persistent://" + namespace + "/P-NP";
        admin.topics().createNonPartitionedTopic(topicNameToWatch);
        admin.namespaces().setInactiveTopicPolicies(namespace,
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true));

        // Remove retention
        admin.namespaces().setRetention(namespace, new RetentionPolicies());
        events.clear();

        Thread.sleep(1500);
        runGC();

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                })
        );

    }

    @Test
    public void testPartitionedTopicAutoGC() throws Exception {
        String topicName = "persistent://" + namespace + "/P-NP";
        topicNameToWatch = topicName + "-partition-1";
        admin.topics().createPartitionedTopic(topicName, 2);
        triggerPartitionsCreation(topicName);
        admin.namespaces().setInactiveTopicPolicies(namespace,
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true));

        // Remove retention
        admin.namespaces().setRetention(namespace, new RetentionPolicies());
        events.clear();

        Thread.sleep(1500);
        runGC();

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(events.toArray(), new String[]{
                        "UNLOAD__BEFORE",
                        "UNLOAD__SUCCESS",
                })
        );

    }

    private void triggerPartitionsCreation(String topicName) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        producer.close();
    }

}
