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
package org.apache.pulsar.broker.system;

import com.google.common.collect.Sets;
import org.apache.bookkeeper.common.util.JsonUtil.ParseJsonException;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.systopic.ActionType;
import org.apache.pulsar.broker.systopic.EventType;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicService;
import org.apache.pulsar.broker.systopic.PulsarEvent;
import org.apache.pulsar.broker.systopic.SystemTopic;
import org.apache.pulsar.broker.systopic.SystemTopicService;
import org.apache.pulsar.broker.systopic.TopicEvent;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class NamespaceEventsSystemTopicServiceTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceEventsSystemTopicServiceTest.class);

    private static final String NAMESPACE1 = "system-topic/namespace-1";
    private static final String NAMESPACE2 = "system-topic/namespace-2";
    private static final String NAMESPACE3 = "system-topic/namespace-3";

    private static final String LOCAL_TOPIC_NAME = "__change_events";

    private NamespaceEventsSystemTopicService systemTopicService;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        prepareData();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetSystemTopic() {

        SystemTopic systemTopicForNamespace1 = systemTopicService.getSystemTopic(NAMESPACE1, EventType.TOPIC_POLICY);
        Assert.assertEquals(systemTopicForNamespace1.getTopicName().getNamespace(), NAMESPACE1);
        Assert.assertEquals(systemTopicForNamespace1.getTopicName().getLocalName(), LOCAL_TOPIC_NAME);

        SystemTopic systemTopicForNamespace2 = systemTopicService.getSystemTopic(NAMESPACE2, EventType.TOPIC_POLICY);
        Assert.assertEquals(systemTopicForNamespace2.getTopicName().getNamespace(), NAMESPACE2);
        Assert.assertEquals(systemTopicForNamespace2.getTopicName().getLocalName(), LOCAL_TOPIC_NAME);

        SystemTopic systemTopicForNamespace3 = systemTopicService.getSystemTopic(NAMESPACE3, EventType.TOPIC_POLICY);
        Assert.assertEquals(systemTopicForNamespace3.getTopicName().getNamespace(), NAMESPACE3);
        Assert.assertEquals(systemTopicForNamespace3.getTopicName().getLocalName(), LOCAL_TOPIC_NAME);

        SystemTopic cachedSystemTopicForNamespace1 = systemTopicService.getSystemTopic(NAMESPACE1, EventType.TOPIC_POLICY);
        Assert.assertSame(cachedSystemTopicForNamespace1, systemTopicForNamespace1);

        SystemTopic cachedSystemTopicForNamespace2 = systemTopicService.getSystemTopic(NAMESPACE2, EventType.TOPIC_POLICY);
        Assert.assertSame(cachedSystemTopicForNamespace2, systemTopicForNamespace2);

        SystemTopic cachedSystemTopicForNamespace3 = systemTopicService.getSystemTopic(NAMESPACE3, EventType.TOPIC_POLICY);
        Assert.assertSame(cachedSystemTopicForNamespace3, systemTopicForNamespace3);
    }

    @Test
    public void testDestroySystemTopic() {
        SystemTopic systemTopicForNamespace1 = systemTopicService.getSystemTopic(NAMESPACE1, EventType.TOPIC_POLICY);
        systemTopicService.invalidate(NAMESPACE1, EventType.TOPIC_POLICY);
        SystemTopic systemTopicForNamespace2 = systemTopicService.getSystemTopic(NAMESPACE1, EventType.TOPIC_POLICY);
        Assert.assertNotSame(systemTopicForNamespace1, systemTopicForNamespace2);
        systemTopicService.invalidate(NAMESPACE1, EventType.TOPIC_POLICY);
    }

    @Test
    public void testSendAndReceiveNamespaceEvents() throws Exception {
        SystemTopic systemTopicForNamespace1 = systemTopicService.getSystemTopic(NAMESPACE1, EventType.TOPIC_POLICY);
        TopicPolicies policies = TopicPolicies.builder()
            .maxProducerPerTopic(10)
            .build();
        PulsarEvent event = PulsarEvent.builder()
            .eventType(EventType.TOPIC_POLICY)
            .actionType(ActionType.INSERT)
            .topicEvent(TopicEvent.builder()
                .domain("persistent")
                .tenant("system-topic")
                .namespace(NamespaceName.get(NAMESPACE1).getLocalName())
                .topic("my-topic")
                .policies(policies)
                .build())
            .build();
        systemTopicForNamespace1.newWriter().write(event);
        SystemTopic.Reader reader = systemTopicForNamespace1.newReader();
        Message<PulsarEvent> received = reader.readNext();
        log.info("Receive pulsar event from system topic : {}", received.getValue());

        // test event send and receive
        Assert.assertEquals(received.getValue(), event);
        Assert.assertEquals(systemTopicForNamespace1.getWriters().size(), 1);
        Assert.assertEquals(systemTopicForNamespace1.getReaders().size(), 1);

        // test new reader read
        SystemTopic.Reader reader1 = systemTopicForNamespace1.newReader();
        Message<PulsarEvent> received1 = reader1.readNext();
        log.info("Receive pulsar event from system topic : {}", received1.getValue());
        Assert.assertEquals(received1.getValue(), event);

        // test writers and readers
        Assert.assertEquals(systemTopicForNamespace1.getReaders().size(), 2);
        SystemTopic.Writer writer = systemTopicForNamespace1.newWriter();
        Assert.assertEquals(systemTopicForNamespace1.getWriters().size(), 2);
        writer.close();
        reader.close();
        Assert.assertEquals(systemTopicForNamespace1.getWriters().size(), 1);
        Assert.assertEquals(systemTopicForNamespace1.getReaders().size(), 1);
        systemTopicForNamespace1.close();
        Assert.assertEquals(systemTopicForNamespace1.getWriters().size(), 0);
        Assert.assertEquals(systemTopicForNamespace1.getReaders().size(), 0);
    }

    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster("test", new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
        admin.tenants().createTenant("system-topic",
            new TenantInfo(Sets.newHashSet(), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.namespaces().createNamespace(NAMESPACE2);
        admin.namespaces().createNamespace(NAMESPACE3);
        systemTopicService = new NamespaceEventsSystemTopicService(pulsarClient);
    }
}
