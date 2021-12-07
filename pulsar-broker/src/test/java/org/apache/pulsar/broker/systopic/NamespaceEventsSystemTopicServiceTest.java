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
package org.apache.pulsar.broker.systopic;

import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceEventsSystemTopicServiceTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceEventsSystemTopicServiceTest.class);

    private static final String NAMESPACE1 = "system-topic/namespace-1";
    private static final String NAMESPACE2 = "system-topic/namespace-2";
    private static final String NAMESPACE3 = "system-topic/namespace-3";

    private NamespaceEventsSystemTopicFactory systemTopicFactory;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        prepareData();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSchemaCompatibility() throws Exception {
        TopicPoliciesSystemTopicClient systemTopicClientForNamespace1 = systemTopicFactory
                .createTopicPoliciesSystemTopicClient(NamespaceName.get(NAMESPACE1));
        String topicName = systemTopicClientForNamespace1.getTopicName().toString();
        @Cleanup
        Reader<byte[]> reader = pulsarClient.newReader(Schema.BYTES)
                .topic(topicName)
                .startMessageId(MessageId.earliest)
                .create();

        PersistentTopic topic =
                (PersistentTopic) pulsar.getBrokerService()
                        .getTopic(topicName, false)
                        .join().get();

        Assert.assertEquals(SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE, topic.getSchemaCompatibilityStrategy());
    }

    @Test
    public void testSendAndReceiveNamespaceEvents() throws Exception {
        TopicPoliciesSystemTopicClient systemTopicClientForNamespace1 = systemTopicFactory
                .createTopicPoliciesSystemTopicClient(NamespaceName.get(NAMESPACE1));
        TopicPolicies policies = TopicPolicies.builder()
            .maxProducerPerTopic(10)
            .build();
        PulsarEvent event = PulsarEvent.builder()
            .eventType(EventType.TOPIC_POLICY)
            .actionType(ActionType.INSERT)
            .topicPoliciesEvent(TopicPoliciesEvent.builder()
                .domain("persistent")
                .tenant("system-topic")
                .namespace(NamespaceName.get(NAMESPACE1).getLocalName())
                .topic("my-topic")
                .policies(policies)
                .build())
            .build();
        systemTopicClientForNamespace1.newWriter().write(event);
        SystemTopicClient.Reader reader = systemTopicClientForNamespace1.newReader();
        Message<PulsarEvent> received = reader.readNext();
        log.info("Receive pulsar event from system topic : {}", received.getValue());

        // test event send and receive
        Assert.assertEquals(received.getValue(), event);
        Assert.assertEquals(systemTopicClientForNamespace1.getWriters().size(), 1);
        Assert.assertEquals(systemTopicClientForNamespace1.getReaders().size(), 1);

        // test new reader read
        SystemTopicClient.Reader reader1 = systemTopicClientForNamespace1.newReader();
        Message<PulsarEvent> received1 = reader1.readNext();
        log.info("Receive pulsar event from system topic : {}", received1.getValue());
        Assert.assertEquals(received1.getValue(), event);

        // test writers and readers
        Assert.assertEquals(systemTopicClientForNamespace1.getReaders().size(), 2);
        SystemTopicClient.Writer writer = systemTopicClientForNamespace1.newWriter();
        Assert.assertEquals(systemTopicClientForNamespace1.getWriters().size(), 2);
        writer.close();
        reader.close();
        Assert.assertEquals(systemTopicClientForNamespace1.getWriters().size(), 1);
        Assert.assertEquals(systemTopicClientForNamespace1.getReaders().size(), 1);
        systemTopicClientForNamespace1.close();
        Assert.assertEquals(systemTopicClientForNamespace1.getWriters().size(), 0);
        Assert.assertEquals(systemTopicClientForNamespace1.getReaders().size(), 0);
    }

    @Test(timeOut = 30000)
    public void checkSystemTopic() throws PulsarAdminException {
        final String systemTopic = "persistent://" + NAMESPACE1 + "/" + EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
        final String normalTopic = "persistent://" + NAMESPACE1 + "/normal_topic";
        admin.topics().createPartitionedTopic(normalTopic, 3);
        TopicName systemTopicName = TopicName.get(systemTopic);
        TopicName normalTopicName = TopicName.get(normalTopic);

        Assert.assertEquals(SystemTopicClient.isSystemTopic(systemTopicName), true);
        Assert.assertEquals(SystemTopicClient.isSystemTopic(normalTopicName), false);
    }

    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("system-topic",
            new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.namespaces().createNamespace(NAMESPACE2);
        admin.namespaces().createNamespace(NAMESPACE3);
        systemTopicFactory = new NamespaceEventsSystemTopicFactory(pulsarClient);
    }
}
