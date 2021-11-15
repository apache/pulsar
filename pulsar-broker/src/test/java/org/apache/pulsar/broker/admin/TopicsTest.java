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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.rest.Topics;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAcks;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

public class TopicsTest extends MockedPulsarServiceBaseTest {

    private Topics topics;
    private final String testLocalCluster = "test";
    private final String testTenant = "my-tenant";
    private final String testNamespace = "my-namespace";
    private final String testTopicName = "my-topic";

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        topics = spy(new Topics());
        topics.setPulsar(pulsar);
        doReturn(TopicDomain.persistent.value()).when(topics).domain();
        doReturn("test-app").when(topics).clientAppId();
        doReturn(mock(AuthenticationDataHttps.class)).when(topics).clientAuthData();
        admin.clusters().createCluster(testLocalCluster, new ClusterDataImpl());
        admin.tenants().createTenant(testTenant, new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet(testLocalCluster)));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace,
                Sets.newHashSet(testLocalCluster));
    }

    @Override
    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProduceToNonPartitionedTopic() throws Exception {
        admin.topics().createNonPartitionedTopic("persistent://" + testTenant + "/"
                + testNamespace + "/" + testTopicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 3);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
    }

    @Test
    public void testProduceToPartitionedTopic() throws Exception {
        admin.topics().createPartitionedTopic("persistent://" + testTenant + "/" + testNamespace
                + "/" + testTopicName + "-p", 5);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:5\",\"eventTime\":1603045262772,\"sequenceId\":5}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:6\",\"eventTime\":1603045262772,\"sequenceId\":6}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:7\",\"eventTime\":1603045262772,\"sequenceId\":7}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:8\",\"eventTime\":1603045262772,\"sequenceId\":8}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:9\",\"eventTime\":1603045262772,\"sequenceId\":9}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:10\",\"eventTime\":1603045262772,\"sequenceId\":10}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName + "-p", false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 10);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        int[] messagePerPartition = new int[5];
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            messagePerPartition[Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2])]++;
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
        for (int index = 0; index < messagePerPartition.length; index++) {
            // We publish to each partition in round robin mode so each partition should get at most 2 message.
            Assert.assertTrue(messagePerPartition[index] <= 2);
        }
    }

    @Test
    public void testProduceToPartitionedTopicSpecificPartition() throws Exception {
        admin.topics().createPartitionedTopic("persistent://" + testTenant + "/"
                + testNamespace + "/" + testTopicName, 5);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopicPartition(asyncResponse, testTenant, testNamespace, testTopicName, 2,false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 4);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), 2);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
    }

    @Test
    public void testProduceFailed() throws Exception {
        admin.topics().createNonPartitionedTopic("persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName);
        pulsar.getBrokerService().getTopic("persistent://" + testTenant + "/" + testNamespace
                + "/" + testTopicName, false).thenAccept(topic -> {
            try {
                PersistentTopic mockPersistentTopic = spy((PersistentTopic) topic.get());
                AtomicInteger count = new AtomicInteger();
                doAnswer(new Answer() {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Topic.PublishContext publishContext = invocationOnMock.getArgument(1);
                        if (count.getAndIncrement() < 2) {
                            publishContext.completed(null, -1, -1);
                        } else {
                            publishContext.completed(new BrokerServiceException.TopicFencedException("Fake exception"),
                                    -1, -1);
                        }
                        return null;
                    }
                }).when(mockPersistentTopic).publishMessage(any(), any());
                BrokerService mockBrokerService = spy(pulsar.getBrokerService());
                doReturn(CompletableFuture.completedFuture(Optional.of(mockPersistentTopic)))
                        .when(mockBrokerService).getTopic(anyString(), anyBoolean());
                doReturn(mockBrokerService).when(pulsar).getBrokerService();
                AsyncResponse asyncResponse = mock(AsyncResponse.class);
                Schema<String> schema = StringSchema.utf8();
                ProducerMessages producerMessages = new ProducerMessages();
                producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                        writeValueAsString(schema.getSchemaInfo()));
                producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                        writeValueAsString(schema.getSchemaInfo()));
                String message = "[" +
                        "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                        "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                        "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                        "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4}]";
                producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                        new TypeReference<List<ProducerMessage>>() {}));
                // Previous request should trigger namespace bundle loading, retry produce.
                topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
                ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
                verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
                Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
                Object responseEntity = responseCaptor.getValue().getEntity();
                Assert.assertTrue(responseEntity instanceof ProducerAcks);
                ProducerAcks response = (ProducerAcks) responseEntity;
                Assert.assertEquals(response.getMessagePublishResults().size(), 4);
                int errorResponse = 0;
                for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
                    int errorCode = response.getMessagePublishResults().get(index).getErrorCode();
                    if (0 == errorCode) {
                        Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                                .getMessageId().split(":")[2]), -1);
                        Assert.assertTrue(response.getMessagePublishResults().get(index)
                                .getMessageId().length() > 0);
                    } else {
                        errorResponse++;
                        Assert.assertEquals(errorCode, 2);
                        Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorMsg(),
                                "org.apache.pulsar.broker.service.BrokerServiceException$"
                                         + "TopicFencedException: Fake exception");
                    }
                }
                // Add entry start to fail after 2nd operation, we published 4 msg so expecting 2 error response.
                Assert.assertTrue(errorResponse == 2);
            } catch (Throwable e) {
                Assert.fail(e.getMessage());
            }
        }).get();
    }

    @Test
    public void testLookUpWithRedirect() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        String requestPath = "/admin/v3/topics/my-tenant/my-namespace/my-topic";
        //create topic on one broker
        admin.topics().createNonPartitionedTopic(topicName);
        PulsarService pulsar2 = startBroker(getDefaultConf());
        doReturn(false).when(topics).isRequestHttps();
        UriInfo uriInfo = mock(UriInfo.class);
        doReturn(requestPath).when(uriInfo).getPath(anyBoolean());
        Whitebox.setInternalState(topics, "uri", uriInfo);
        //do produce on another broker
        topics.setPulsar(pulsar2);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(Schema.INT64.getSchemaInfo()));
        String message = "[]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        // Verify got redirect response
        Assert.assertEquals(responseCaptor.getValue().getStatusInfo(), Response.Status.TEMPORARY_REDIRECT);
        // Verify URI point to address of broker the topic was created on
        Assert.assertEquals(responseCaptor.getValue().getLocation().toString(),
                pulsar.getWebServiceAddress() + requestPath);
    }
    
    @Test
    public void testLookUpWithException() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        NamespaceService nameSpaceService = mock(NamespaceService.class);
        CompletableFuture future = new CompletableFuture();
        future.completeExceptionally(new BrokerServiceException("Fake Exception"));
        CompletableFuture existFuture = new CompletableFuture();
        existFuture.complete(true);
        doReturn(future).when(nameSpaceService).getBrokerServiceUrlAsync(any(), any());
        doReturn(existFuture).when(nameSpaceService).checkTopicExists(any());
        doReturn(nameSpaceService).when(pulsar).getNamespaceService();
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(Schema.INT64.getSchemaInfo()));
        String message = "[]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<RestException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getMessage(), "Can't find owner of given topic.");
    }

    @Test
    public void testLookUpTopicNotExist() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        NamespaceService nameSpaceService = mock(NamespaceService.class);
        CompletableFuture existFuture = new CompletableFuture();
        existFuture.complete(false);
        doReturn(existFuture).when(nameSpaceService).checkTopicExists(any());
        doReturn(nameSpaceService).when(pulsar).getNamespaceService();
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(Schema.INT64.getSchemaInfo()));
        String message = "[]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<RestException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getMessage(), "Fail to publish message: Topic not exist");
    }

    @Test
    public void testProduceWithLongSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Consumer consumer = pulsarClient.newConsumer(Schema.INT64)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(Schema.INT64.getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"111111111111\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"222222222222\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"333333333333\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"key\":\"my-key\",\"payload\":\"444444444444\",\"eventTime\":1603045262772,\"sequenceId\":4}," +
                "{\"key\":\"my-key\",\"payload\":\"555555555555\",\"eventTime\":1603045262772,\"sequenceId\":5}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 5);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }

        List<Long> expectedMsg = Arrays.asList(111111111111l, 222222222222l, 333333333333l, 444444444444l, 555555555555l);
        Message<Long> msg = null;
        // Assert all messages published by REST producer can be received by consumer in expected order.
        for (int i = 0; i < 5; i++) {
            msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertEquals(expectedMsg.get(i), Schema.INT64.decode(msg.getData()));
            Assert.assertEquals("my-key", msg.getKey());
        }
    }

    // Default schema is String schema
    @Test
    public void testProduceNoSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Consumer consumer = pulsarClient.newConsumer(StringSchema.utf8())
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:5\",\"eventTime\":1603045262772,\"sequenceId\":5}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 5);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }

        List<String> expectedMsg = Arrays.asList("RestProducer:1", "RestProducer:2", "RestProducer:3", "RestProducer:4",
                "RestProducer:5");
        Message<String> msg = null;
        // Assert all messages published by REST producer can be received by consumer in expected order.
        for (int i = 0; i < 5; i++) {
            msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertEquals(expectedMsg.get(i), StringSchema.utf8().decode(msg.getData()));
            Assert.assertEquals("my-key", msg.getKey());
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Seller {
        public String state;
        public String street;
        public long zipCode;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class PC {
        public String brand;
        public String model;
        public int year;
        public GPU gpu;
        public Seller seller;
    }

    private enum GPU {
        AMD, NVIDIA
    }

    @Test
    public void testProduceWithJsonSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        GenericSchema jsonSchema = GenericJsonSchema.of(JSONSchema.of(SchemaDefinition.builder()
                .withPojo(PC.class).build()).getSchemaInfo());
        PC pc  = new PC("dell", "alienware", 2021, GPU.AMD,
                new Seller("WA", "main street", 98004));
        PC anotherPc  = new PC("asus", "rog", 2020, GPU.NVIDIA,
                new Seller("CA", "back street", 90232));
        Consumer consumer = pulsarClient.newConsumer(jsonSchema)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(jsonSchema.getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\""
                + ObjectMapperFactory.getThreadLocal().writeValueAsString(pc).replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\""
                + ObjectMapperFactory.getThreadLocal().writeValueAsString(anotherPc).replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":2}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace,
                testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 2);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }

        List<PC> expected = Arrays.asList(pc, anotherPc);
        Message<String> msg = null;
        // Assert all messages published by REST producer can be received by consumer in expected order.
        for (int i = 0; i < 2; i++) {
            msg = consumer.receive(2, TimeUnit.SECONDS);
            PC msgPc = ObjectMapperFactory.getThreadLocal().
                    treeToValue(((GenericJsonRecord)jsonSchema.decode(msg.getData())).getJsonNode(), PC.class);
            Assert.assertEquals(msgPc.brand, expected.get(i).brand);
            Assert.assertEquals(msgPc.model, expected.get(i).model);
            Assert.assertEquals(msgPc.year, expected.get(i).year);
            Assert.assertEquals(msgPc.gpu, expected.get(i).gpu);
            Assert.assertEquals(msgPc.seller.state, expected.get(i).seller.state);
            Assert.assertEquals(msgPc.seller.street, expected.get(i).seller.street);
            Assert.assertEquals(msgPc.seller.zipCode, expected.get(i).seller.zipCode);
            Assert.assertEquals("my-key", msg.getKey());
        }
    }

    @Test
    public void testProduceWithAvroSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        GenericSchemaImpl avroSchema = GenericAvroSchema.of(AvroSchema.of(SchemaDefinition.builder()
                .withPojo(PC.class).build()).getSchemaInfo());
        PC pc  = new PC("dell", "alienware", 2021, GPU.AMD,
                new Seller("WA", "main street", 98004));
        PC anotherPc  = new PC("asus", "rog", 2020, GPU.NVIDIA,
                new Seller("CA", "back street", 90232));
        Consumer consumer = pulsarClient.newConsumer(avroSchema)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(avroSchema.getSchemaInfo()));

        ReflectDatumWriter<PC> datumWriter = new ReflectDatumWriter(avroSchema.getAvroSchema());
        ByteArrayOutputStream outputStream1 = new ByteArrayOutputStream();
        ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();

        JsonEncoder encoder1 = EncoderFactory.get().jsonEncoder(avroSchema.getAvroSchema(), outputStream1);
        JsonEncoder encoder2 = EncoderFactory.get().jsonEncoder(avroSchema.getAvroSchema(), outputStream2);

        datumWriter.write(pc, encoder1);
        encoder1.flush();
        datumWriter.write(anotherPc, encoder2);
        encoder2.flush();

        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\""
                + outputStream1.toString().replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\""
                + outputStream2.toString().replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":2}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace,
                testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 2);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }

        List<PC> expected = Arrays.asList(pc, anotherPc);
        Message<String> msg = null;
        // Assert all messages published by REST producer can be received by consumer in expected order.
        for (int i = 0; i < 2; i++) {
            msg = consumer.receive(2, TimeUnit.SECONDS);
            GenericAvroRecord avroRecord = (GenericAvroRecord) avroSchema.decode(msg.getData());
            Assert.assertEquals(((Utf8)avroRecord.getAvroRecord().get("brand")).toString(), expected.get(i).brand);
            Assert.assertEquals(((Utf8)avroRecord.getAvroRecord().get("model")).toString(), expected.get(i).model);
            Assert.assertEquals((int)avroRecord.getAvroRecord().get("year"), expected.get(i).year);
            Assert.assertEquals(((GenericData.EnumSymbol)avroRecord.getAvroRecord().get("gpu")).toString(), expected.get(i).gpu.toString());
            Assert.assertEquals(((Utf8)((GenericRecord)avroRecord.getAvroRecord().get("seller")).get("state")).toString(), expected.get(i).seller.state);
            Assert.assertEquals(((Utf8)((GenericRecord)avroRecord.getAvroRecord().get("seller")).get("street")).toString(), expected.get(i).seller.street);
            Assert.assertEquals(((GenericRecord)avroRecord.getAvroRecord().get("seller")).get("zipCode"), expected.get(i).seller.zipCode);
            Assert.assertEquals("my-key", msg.getKey());
        }
    }

    @Test
    public void testProduceWithRestAndClientThenConsumeWithClient() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema keyValueSchema = KeyValueSchemaImpl.of(StringSchema.utf8(), StringSchema.utf8(),
                                                KeyValueEncodingType.SEPARATED);
        Producer producer = pulsarClient.newProducer(keyValueSchema)
                                        .topic(topicName)
                                        .create();
        Consumer consumer = pulsarClient.newConsumer(keyValueSchema)
                                        .topic(topicName)
                                        .subscriptionName("my-sub")
                                        .subscriptionType(SubscriptionType.Exclusive)
                                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                                        .subscribe();
        for (int i = 0; i < 3; i++) {
            producer.newMessage(keyValueSchema)
                    .value(new KeyValue<>("my-key", "ClientProducer:" + i))
                    .send();
        }
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 3);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }

        List<String> expectedMsg = Arrays.asList("ClientProducer:0", "ClientProducer:1", "ClientProducer:2",
                                                 "RestProducer:1", "RestProducer:2", "RestProducer:3");
        Message<String> msg = null;
        // Assert both messages published by client producer and REST producer can be received
        // by consumer in expected order.
        for (int i = 0; i < 6; i++) {
            msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertEquals(expectedMsg.get(i), StringSchema.utf8().decode(msg.getData()));
            Assert.assertEquals("bXkta2V5", msg.getKey());
        }
    }

    @Test
    public void testProduceWithRestThenConsumeWithClient() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema keyValueSchema = KeyValueSchemaImpl.of(StringSchema.utf8(), StringSchema.utf8(),
                KeyValueEncodingType.SEPARATED);
        Consumer consumer = pulsarClient.newConsumer(keyValueSchema)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:5\",\"eventTime\":1603045262772,\"sequenceId\":5}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 5);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
        // Specify schema version to use existing schema.
        producerMessages = new ProducerMessages();
        producerMessages.setSchemaVersion(response.getSchemaVersion());
        message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:6\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:7\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:8\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:9\",\"eventTime\":1603045262772,\"sequenceId\":4}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:10\",\"eventTime\":1603045262772,\"sequenceId\":5}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 5);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(Integer.parseInt(response.getMessagePublishResults().get(index)
                    .getMessageId().split(":")[2]), -1);
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }

        List<String> expectedMsg = Arrays.asList("RestProducer:1", "RestProducer:2", "RestProducer:3",
                "RestProducer:4", "RestProducer:5", "RestProducer:6",
                "RestProducer:7", "RestProducer:8", "RestProducer:9",
                "RestProducer:10");
        Message<String> msg = null;
        // Assert all messages published by REST producer can be received by consumer in expected order.
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertEquals(expectedMsg.get(i), StringSchema.utf8().decode(msg.getData()));
            Assert.assertEquals("bXkta2V5", msg.getKey());
        }
    }

    @Test
    public void testProduceWithInCompatibleSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Producer producer = pulsarClient.newProducer(StringSchema.utf8())
                .topic(topicName)
                .create();

        for (int i = 0; i < 3; i++) {
            producer.send("message");
        }
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false, producerMessages);
        ArgumentCaptor<RestException> responseCaptor = ArgumentCaptor.forClass(RestException.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertTrue(responseCaptor.getValue().getMessage().startsWith("Fail to publish message:"
                + "java.util.concurrent.ExecutionException: org.apache.pulsar.broker.service.schema.exceptions."
                + "SchemaException: Unable to add schema SchemaData(type=KEY_VALUE, isDeleted=false, "
                + "timestamp="));
        Assert.assertTrue(responseCaptor.getValue().getMessage().endsWith(
                "user=Rest Producer, data=[0, 0, 0, 0, 0, 0, 0, 0], "
                + "props={key.schema.properties={\"__charset\":\"UTF-8\"}, value.schema.properties={\"__charset\":"
                + "\"UTF-8\"}, value.schema.type=STRING, key.schema.name=String, value.schema.name=String, "
                + "kv.encoding.type=SEPARATED, key.schema.type=STRING}) to topic persistent:"
                + "//my-tenant/my-namespace/my-topic"));
    }
}
