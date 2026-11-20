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

import java.util.UUID;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.awaitility.Awaitility;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-admin")
public class TopicMessageTTLTest extends SharedPulsarBaseTest {

    private String myNamespace;
    private String testTopic;
    private int brokerTtlDurationDefaultInSeconds;

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @BeforeMethod
    public void setupTestTopic() throws Exception {
        // the broker-level TTL: the shared broker reads it when a topic loads and for the applied TTL
        brokerTtlDurationDefaultInSeconds = getConfig().getTtlDurationDefaultInSeconds();
        getConfig().setTtlDurationDefaultInSeconds(3600);

        myNamespace = getNamespace();
        testTopic = "persistent://" + myNamespace + "/test-topic-message-ttl";
        admin.topics().createPartitionedTopic(testTopic, 2);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(myNamespace + "/" + "dummy-topic").create();
        producer.close();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreTtlDurationDefault() {
        getConfig().setTtlDurationDefaultInSeconds(brokerTtlDurationDefaultInSeconds);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSetThenRemoveMessageTTL() throws Exception {
        admin.topics().setMessageTTL(testTopic, 100);
        log.info().attr("topic", testTopic).log("Message TTL set success on topic");

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(admin.topics().getMessageTTL(testTopic), Integer.valueOf(100)));
        Integer messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info().attr("topic", testTopic).attr("messageTTL", messageTTL)
                .log("Message TTL get on topic");

        admin.topics().removeMessageTTL(testTopic);
        Awaitility.await().untilAsserted(() -> Assert.assertNull(admin.topics().getMessageTTL(testTopic)));
        messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info().attr("topic", testTopic).attr("messageTTL", messageTTL)
                .log("Message TTL get on topic");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSetInvalidMessageTTL() throws Exception {
        try {
            admin.topics().setMessageTTL(testTopic, -100);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.PRECONDITION_FAILED_412);
        }

        try {
            admin.topics().setMessageTTL(testTopic, (int) 2147483650L);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.PRECONDITION_FAILED_412);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetMessageTTL() throws Exception {
        // Check default topic level message TTL.
        Integer messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info().attr("topic", testTopic).attr("messageTTL", messageTTL)
                .log("Message TTL get on topic");
        Assert.assertNull(messageTTL);

        admin.topics().setMessageTTL(testTopic, 200);
        log.info().attr("topic", testTopic).log("Message TTL set success on topic");

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(admin.topics().getMessageTTL(testTopic), Integer.valueOf(200)));
        messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info().attr("topic", testTopic).attr("messageTTL", messageTTL)
                .log("Message TTL get on topic");
    }
    @SuppressWarnings("deprecation")

    @Test
    public void testTopicPolicyDisabled() throws Exception {
        // the shared broker reads it (after its start) only when it serves a topic policy request
        boolean topicLevelPoliciesEnabled = getConfig().isTopicLevelPoliciesEnabled();
        getConfig().setTopicLevelPoliciesEnabled(false);
        try {
            try {
                admin.topics().getMessageTTL(testTopic);
                Assert.fail();
            } catch (PulsarAdminException e) {
                Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
            }

            try {
                admin.topics().setMessageTTL(testTopic, 200);
                Assert.fail();
            } catch (PulsarAdminException e) {
                Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
            }
        } finally {
            getConfig().setTopicLevelPoliciesEnabled(topicLevelPoliciesEnabled);
        }
    }

    @Test(timeOut = 20000)
    public void testDifferentLevelPolicyPriority() throws Exception {
        final String topicName = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic = (PersistentTopic) getTopicIfExists(topicName).get().get();

        Integer namespaceMessageTTL = admin.namespaces().getNamespaceMessageTTL(myNamespace);
        Assert.assertNull(namespaceMessageTTL);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 3600));

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 10);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 10));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 10));

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 0);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 0));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 0));

        admin.namespaces().removeNamespaceMessageTTL(myNamespace);
        Awaitility.await().untilAsserted(()
                -> Assert.assertNull(admin.namespaces().getNamespaceMessageTTL(myNamespace)));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 3600));
    }

    @Test
    public void testNamespaceTTL() throws Exception {
        String myNamespace = createNewNamespace();

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 10);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 10));

        admin.namespaces().removeNamespaceMessageTTL(myNamespace);
        Awaitility.await().untilAsserted(()
                -> Assert.assertNull(admin.namespaces().getNamespaceMessageTTL(myNamespace)));
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 20000)
    public void testDifferentLevelPolicyApplied() throws Exception {
        final String topicName = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopicIfExists(topicName).get().get();
        //namespace-level default value is null
        Integer namespaceMessageTTL = admin.namespaces().getNamespaceMessageTTL(myNamespace);
        Assert.assertNull(namespaceMessageTTL);
        //topic-level default value is null
        Integer topicMessageTTL = admin.topics().getMessageTTL(topicName);
        Assert.assertNull(topicMessageTTL);
        //use broker-level by default
        int topicMessageTTLApplied = admin.topics().getMessageTTL(topicName, true);
        Assert.assertEquals(topicMessageTTLApplied, 3600);

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 10);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 10));
        topicMessageTTLApplied = admin.topics().getMessageTTL(topicName, true);
        Assert.assertEquals(topicMessageTTLApplied, 10);

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 0);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 0));
        topicMessageTTLApplied = admin.topics().getMessageTTL(topicName, true);
        Assert.assertEquals(topicMessageTTLApplied, 0);

        admin.topics().setMessageTTL(topicName, 20);
        Awaitility.await().untilAsserted(()
                -> Assert.assertNotNull(admin.topics().getMessageTTL(topicName)));
        topicMessageTTLApplied = admin.topics().getMessageTTL(topicName, true);
        Assert.assertEquals(topicMessageTTLApplied, 20);

        admin.namespaces().removeNamespaceMessageTTL(myNamespace);
        admin.topics().removeMessageTTL(topicName);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.topics().getMessageTTL(topicName, true).intValue(), 3600));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 3600));
    }

}
