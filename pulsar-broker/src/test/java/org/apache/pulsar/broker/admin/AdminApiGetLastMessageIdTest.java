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
package org.apache.pulsar.broker.admin;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.UriInfo;
import org.apache.pulsar.broker.admin.v2.PersistentTopics;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AdminApiGetLastMessageIdTest extends MockedPulsarServiceBaseTest {

    private PersistentTopics persistentTopics;
    private final String testTenant = "my-tenant";
    private final String testLocalCluster = "use";
    private final String testNamespace = "my-namespace";
    protected Field uriField;
    protected UriInfo uriInfo;

    @BeforeClass
    public void initPersistentTopics() throws Exception {
        uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriInfo = mock(UriInfo.class);
    }

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("prop",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop/ns-abc");
        admin.namespaces().setNamespaceReplicationClusters("prop/ns-abc", Sets.newHashSet("test"));
        persistentTopics = spy(new PersistentTopics());
        persistentTopics.setServletContext(new MockServletContext());
        persistentTopics.setPulsar(pulsar);

        doReturn(false).when(persistentTopics).isRequestHttps();
        doReturn(null).when(persistentTopics).originalPrincipal();
        doReturn("test").when(persistentTopics).clientAppId();
        doReturn("persistent").when(persistentTopics).domain();
        doNothing().when(persistentTopics).validateAdminAccessForTenant(this.testTenant);
        doReturn(mock(AuthenticationDataHttps.class)).when(persistentTopics).clientAuthData();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetLastMessageId() throws Exception {
        final MessageId[] id = new MessageId[1];
        id[0] = null;
        MessageId messageId = null;
        AsyncResponse asyncResponse = new AsyncResponse() {
            @Override
            public boolean resume(Object response) {
                id[0] = (MessageId) response;
                return false;
            }

            @Override
            public boolean resume(Throwable response) {
                return false;
            }

            @Override
            public boolean cancel() {
                return false;
            }

            @Override
            public boolean cancel(int retryAfter) {
                return false;
            }

            @Override
            public boolean cancel(Date retryAfter) {
                return false;
            }

            @Override
            public boolean isSuspended() {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public boolean setTimeout(long time, TimeUnit unit) {
                return false;
            }

            @Override
            public void setTimeoutHandler(TimeoutHandler handler) {

            }

            @Override
            public Collection<Class<?>> register(Class<?> callback) {
                return null;
            }

            @Override
            public Map<Class<?>, Collection<Class<?>>> register(Class<?> callback, Class<?>... callbacks) {
                return null;
            }

            @Override
            public Collection<Class<?>> register(Object callback) {
                return null;
            }

            @Override
            public Map<Class<?>, Collection<Class<?>>> register(Object callback, Object... callbacks) {
                return null;
            }
        };
        try {
            persistentTopics.getLastMessageId(asyncResponse, testTenant,
                    testNamespace, "my-topic", true);
        } catch (Exception e) {
            //System.out.println(e.getMessage());
            Assert.assertEquals("Topic not found", e.getMessage());
        }

        String key = "legendtkl";
        final String topicName = "persistent://prop/ns-abc/my-topic";
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        // 3. Publish message and get message id
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        persistentTopics.getLastMessageId(asyncResponse, "prop", "ns-abc", "my-topic", true);
        Awaitility.await().until(() -> id[0] != null);
        Assert.assertTrue(((MessageIdImpl)id[0]).getLedgerId() >= 0);
        Assert.assertEquals(numberOfMessages-1, ((MessageIdImpl)id[0]).getEntryId());
        messageId = id[0];


        // send more numberOfMessages messages, the last message id should be numberOfMessages*2-1
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }
        persistentTopics.getLastMessageId(asyncResponse, "prop", "ns-abc", "my-topic", true);
        while (id[0] == messageId) {
            Thread.sleep(1);
        }
        Assert.assertTrue(((MessageIdImpl)id[0]).getLedgerId() > 0);
        Assert.assertEquals( 2 * numberOfMessages -1, ((MessageIdImpl)id[0]).getEntryId());
    }

    /**
     * If a topic only have non-durable subscriptions or mark delete position of all the durable subscriptions
     * are reached the LAC, all the ledgers except the current ledger will be deleted. Since the current ledger may not
     * have any data, so the test is to ensure the get last message ID API can work in this case.
     *
     * In this case, the we should return the message ID (-1, -1).
     */
    @Test
    public void testGetLastMessageIdWhenTopicWithoutData() throws Exception {
        final String topic = "persistent://prop/ns-abc/testGetLastMessageIdWhenTopicWithoutData-" + UUID.randomUUID();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send("Message - " + i);
        }
        // To trigger the ledger rollover
        admin.topics().unload(topic);
        Topic topicRef = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        ((PersistentTopic) topicRef).getManagedLedger().trimConsumedLedgersInBackground(new CompletableFuture<>());
        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic);
            Assert.assertEquals(stats.ledgers.size(), 1);
        });
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create();
        // This will call the get last message ID api.
        boolean hasMessage = reader.hasMessageAvailable();
        Assert.assertFalse(hasMessage);
        MessageId messageId = admin.topics().getLastMessageId(topic);
        Assert.assertEquals(messageId.toString(), "-1:-1:-1");
    }
}
