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

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.rest.Topics;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.SharedPulsarCluster;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
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
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAck;
import org.apache.pulsar.websocket.data.ProducerAcks;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * REST produce tests on the shared broker, each in a namespace of its own. {@link TopicsOwnBrokerTest} has the ones
 * that replace broker services or start a second broker.
 */
public class TopicsTest extends SharedPulsarBaseTest {

    private Topics topics;
    private final String testTenant = SharedPulsarCluster.TENANT_NAME;
    private String testNamespace;
    private final String testTopicName = "my-topic";
    private boolean preciseTimeBasedBacklogQuotaCheck;

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @BeforeMethod
    public void setupTopics() throws Exception {
        testNamespace = NamespaceName.get(getNamespace()).getLocalName();
        // as on a broker of its own, which doesn't deduplicate by default: REST produce publishes without a producer
        // name, and the shared broker deduplicates every namespace
        admin.namespaces().setDeduplicationStatus(getNamespace(), false);
        preciseTimeBasedBacklogQuotaCheck = getConfig().isPreciseTimeBasedBacklogQuotaCheck();
        topics = spy(new Topics());
        topics.setPulsar(getPulsar());
        HttpServletRequest request = mock(HttpServletRequest.class);
        doReturn("http").when(request).getScheme();
        topics.setHttpRequest(request);
        doReturn(TopicDomain.persistent.value()).when(topics).domain();
        doReturn("test-app").when(topics).clientAppId();
        doReturn(mock(AuthenticationDataHttps.class)).when(topics).clientAuthData();
        doReturn(null).when(topics).getWebServiceListenerName();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreConfiguration() {
        // testProduceWithBacklogQuotaTimeExceeded changes it; the shared broker reads it on each backlog quota check
        getConfig().setPreciseTimeBasedBacklogQuotaCheck(preciseTimeBasedBacklogQuotaCheck);
    }

    @Test
    public void testProduceToNonPartitionedTopic() throws Exception {
        admin.topics().createNonPartitionedTopic("persistent://" + testTenant + "/"
                + testNamespace + "/" + testTopicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
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

    static List<ProducerMessage> createMessages(String message) throws JsonProcessingException {
        return ObjectMapperFactory.getMapper().reader()
                .forType(new TypeReference<List<ProducerMessage>>() {
                }).readValue(message);
    }

    @DataProvider(name = "partitionNumbers")
    public Object[][] partitionNumbers() {
        return new Object[][] {
            //produce to single-partitioned topic
            {1},
            {5},
        };
    }

    @Test(dataProvider = "partitionNumbers")
    public void testProduceToPartitionedTopic(int numPartitions) throws Exception {
        admin.topics().createPartitionedTopic("persistent://" + testTenant + "/" + testNamespace
                + "/" + testTopicName + "-p", numPartitions);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:5\",\"eventTime\":1603045262772,\"sequenceId\":5},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:6\",\"eventTime\":1603045262772,\"sequenceId\":6},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:7\",\"eventTime\":1603045262772,\"sequenceId\":7},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:8\",\"eventTime\":1603045262772,\"sequenceId\":8},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:9\",\"eventTime\":1603045262772,\"sequenceId\":9},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:10\",\"eventTime\":1603045262772,\"sequenceId\":10}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName + "-p",
                false, producerMessages);
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
            Assert.assertTrue(messagePerPartition[index] <= 10 / numPartitions);
        }
    }

    @Test
    public void testProduceToPartitionedTopicSpecificPartition() throws Exception {
        admin.topics().createPartitionedTopic("persistent://" + testTenant + "/"
                + testNamespace + "/" + testTopicName, 5);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopicPartition(asyncResponse, testTenant, testNamespace, testTopicName,
                2, false, producerMessages);
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
    public void testProduceWithDeduplicationEnabled() throws Exception {
        String namespace = testTenant + "/" + testNamespace;
        String topicName = "persistent://" + namespace + "/" + testTopicName;
        admin.namespaces().setDeduplicationStatus(namespace, true);
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic topic = (PersistentTopic) getTopicIfExists(topicName).get().orElseThrow();
        await().until(() -> topic.getMessageDeduplication().isEnabled());
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setMessages(createMessages("["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"sequenceId\":2}]"));
        // The REST API publishes without a producer, so the topic doesn't deduplicate its messages: the same request
        // sent twice publishes the messages twice
        Set<String> messageIds = new HashSet<>();
        for (int request = 0; request < 2; request++) {
            AsyncResponse asyncResponse = mock(AsyncResponse.class);
            topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                    false, producerMessages);
            ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
            verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
            Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
            ProducerAcks response = (ProducerAcks) responseCaptor.getValue().getEntity();
            Assert.assertEquals(response.getMessagePublishResults().size(), 2);
            for (ProducerAck ack : response.getMessagePublishResults()) {
                Assert.assertEquals(ack.getErrorCode(), 0, ack.getErrorMsg());
                messageIds.add(ack.getMessageId());
            }
        }
        // A duplicate would be acknowledged with the message ID -1:-1, without being persisted
        Assert.assertEquals(messageIds.size(), 4, messageIds.toString());
        Assert.assertEquals(topic.getNumberOfEntries(), 4);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testProduceWithLongSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(Schema.INT64)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(Schema.INT64.getSchemaInfo()));
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"111111111111\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"222222222222\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"333333333333\",\"eventTime\":1603045262772,\"sequenceId\":3},"
                + "{\"key\":\"my-key\",\"payload\":\"444444444444\",\"eventTime\":1603045262772,\"sequenceId\":4},"
                + "{\"key\":\"my-key\",\"payload\":\"555555555555\",\"eventTime\":1603045262772,\"sequenceId\":5}]";
        producerMessages.setMessages(createMessages(message));
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

        List<Long> expectedMsg = Arrays.asList(111111111111L, 222222222222L, 333333333333L, 444444444444L,
                555555555555L);
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
    @SuppressWarnings("unchecked")
    public void testProduceNoSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(StringSchema.utf8())
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:5\",\"eventTime\":1603045262772,\"sequenceId\":5}]";
        producerMessages.setMessages(createMessages(message));
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
    @SuppressWarnings("unchecked")
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
        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(jsonSchema)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(jsonSchema.getSchemaInfo()));
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\""
                + ObjectMapperFactory.getMapper().writer().writeValueAsString(pc).replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\""
                + ObjectMapperFactory.getMapper().writer().writeValueAsString(anotherPc).replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":2}]";
        producerMessages.setMessages(createMessages(message));
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
            PC msgPc = ObjectMapperFactory.getMapper().getObjectMapper().
                    treeToValue(((GenericJsonRecord) jsonSchema.decode(msg.getData())).getJsonNode(), PC.class);
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
    @SuppressWarnings("unchecked")
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
        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(avroSchema)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
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

        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\""
                + outputStream1.toString().replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\""
                + outputStream2.toString().replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":2}]";
        producerMessages.setMessages(createMessages(message));
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
            Assert.assertEquals(((Utf8) avroRecord.getAvroRecord().get("brand")).toString(), expected.get(i).brand);
            Assert.assertEquals(((Utf8) avroRecord.getAvroRecord().get("model")).toString(), expected.get(i).model);
            Assert.assertEquals((int) avroRecord.getAvroRecord().get("year"), expected.get(i).year);
            Assert.assertEquals(((GenericData.EnumSymbol) avroRecord.getAvroRecord().get("gpu")).toString(),
                    expected.get(i).gpu.toString());
            Assert.assertEquals(((Utf8) ((GenericRecord) avroRecord.getAvroRecord().get("seller")).get("state"))
                    .toString(), expected.get(i).seller.state);
            Assert.assertEquals(((Utf8) ((GenericRecord) avroRecord.getAvroRecord().get("seller")).get("street"))
                    .toString(), expected.get(i).seller.street);
            Assert.assertEquals(((GenericRecord) avroRecord.getAvroRecord().get("seller")).get("zipCode"),
                    expected.get(i).seller.zipCode);
            Assert.assertEquals("my-key", msg.getKey());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProduceWithRestAndClientThenConsumeWithClient() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema keyValueSchema = KeyValueSchemaImpl.of(StringSchema.utf8(), StringSchema.utf8(),
                                                KeyValueEncodingType.SEPARATED);
        @Cleanup
        Producer producer = pulsarClient.newProducer(keyValueSchema)
                                        .topic(topicName)
                                        .create();
        @Cleanup
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
        producerMessages.setKeySchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
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
    @SuppressWarnings("unchecked")
    public void testProduceWithRestThenConsumeWithClient() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        Schema keyValueSchema = KeyValueSchemaImpl.of(StringSchema.utf8(), StringSchema.utf8(),
                KeyValueEncodingType.SEPARATED);
        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(keyValueSchema)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:5\",\"eventTime\":1603045262772,\"sequenceId\":5}]";
        producerMessages.setMessages(createMessages(message));
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
        message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:6\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:7\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:8\",\"eventTime\":1603045262772,\"sequenceId\":3},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:9\",\"eventTime\":1603045262772,\"sequenceId\":4},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:10\",\"eventTime\":1603045262772,\"sequenceId\":5}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
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
    @SuppressWarnings("unchecked")
    public void testProduceWithInCompatibleSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        @Cleanup
        Producer producer = pulsarClient.newProducer(StringSchema.utf8())
                .topic(topicName)
                .create();

        for (int i = 0; i < 3; i++) {
            producer.send("message");
        }
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getMapper().getObjectMapper().
                writeValueAsString(StringSchema.utf8().getSchemaInfo()));
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
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
                + "kv.encoding.type=SEPARATED, key.schema.type=STRING}) to topic " + topicName));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProduceWithAutoConsumeSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        GenericSchema jsonSchema = GenericJsonSchema.of(JSONSchema.of(SchemaDefinition.builder()
                .withPojo(PC.class).build()).getSchemaInfo());
        // use producer to create topic schema
        Producer producer = pulsarClient.newProducer(jsonSchema).topic(topicName).create();
        producer.close();

        PC pc = new PC("dell", "alienware", 2021, GPU.AMD,
                new Seller("WA", "main street", 98004));
        PC anotherPc = new PC("asus", "rog", 2020, GPU.NVIDIA,
                new Seller("CA", "back street", 90232));

        @Cleanup
        Consumer<?> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(topicName)
                .subscriptionName("auto-schema-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        SchemaInfoWithVersion schemaInfoWithVersion = admin.schemas().getSchemaInfoWithVersion(topicName);
        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setSchemaVersion(schemaInfoWithVersion.getVersion());
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\""
                + ObjectMapperFactory.getMapper().writer().writeValueAsString(pc).replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\""
                + ObjectMapperFactory.getMapper().writer().writeValueAsString(anotherPc).replace("\"", "\\\"")
                + "\",\"eventTime\":1603045262772,\"sequenceId\":2}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName, false,
                producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());

        List<PC> expected = Arrays.asList(pc, anotherPc);
        for (int i = 0; i < 2; i++) {
            Message<?> msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertTrue(msg.getValue() instanceof GenericJsonRecord);
            GenericJsonRecord genericJsonRecord = (GenericJsonRecord) msg.getValue();
            Assert.assertEquals(genericJsonRecord.getField("brand"), expected.get(i).getBrand());
            Assert.assertEquals(genericJsonRecord.getField("model"), expected.get(i).getModel());
            Number year = (Number) genericJsonRecord.getField("year");
            Assert.assertEquals(year.intValue(), expected.get(i).getYear());
            Assert.assertEquals(genericJsonRecord.getField("gpu"), expected.get(i).getGpu().name());
            Object seller = genericJsonRecord.getField("seller");
            Assert.assertTrue(seller instanceof GenericJsonRecord);
            GenericJsonRecord sellerGenericJsonRecord = (GenericJsonRecord) seller;
            Assert.assertEquals(sellerGenericJsonRecord.getField("state"), expected.get(i).getSeller().getState());
            Assert.assertEquals(sellerGenericJsonRecord.getField("street"), expected.get(i).getSeller().getStreet());
            Number zipCode = (Number) sellerGenericJsonRecord.getField("zipCode");
            Assert.assertEquals(zipCode.longValue(), expected.get(i).getSeller().getZipCode());
        }
    }

    @Test
    public void testProduceWithBacklogQuotaSizeExceeded() throws Exception {
        String namespaceName = testTenant + "/" + testNamespace;
        String topicName = "persistent://" + namespaceName + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(0)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build();
        admin.namespaces().setBacklogQuota(namespaceName, backlogQuota,
                BacklogQuota.BacklogQuotaType.destination_storage);
        // the loaded topic gets the namespace's new quota asynchronously
        Topic topic = getTopic(topicName, false).get().orElseThrow();
        await().untilAsserted(() -> Assert.assertEquals(
                topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage).getLimitSize(), 0));

        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        String message = "[{\"payload\":\"rest-produce\"}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 1);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 2);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getErrorMsg()
                    .contains("Cannot create producer on topic with backlog quota exceeded"));
        }
    }

    @Test
    public void testProduceToPartitionedTopicWithBacklogEvictionPolicy() throws Exception {
        String namespaceName = testTenant + "/" + testNamespace;
        String topicName = "persistent://" + namespaceName + "/" + testTopicName;
        admin.topics().createPartitionedTopic(topicName, 3);
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(0)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        admin.namespaces().setBacklogQuota(namespaceName, backlogQuota,
                BacklogQuota.BacklogQuotaType.destination_storage);

        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        StringBuilder message = new StringBuilder("[");
        for (int i = 0; i < 9; i++) {
            message.append(i == 0 ? "" : ",").append("{\"payload\":\"rest-produce-").append(i).append("\"}");
        }
        producerMessages.setMessages(createMessages(message.append("]").toString()));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        ProducerAcks response = (ProducerAcks) responseCaptor.getValue().getEntity();
        Assert.assertEquals(response.getMessagePublishResults().size(), 9);
        // The eviction policy never rejects producers, and each partition keeps the request order.
        Map<Integer, Long> lastEntryIdByPartition = new HashMap<>();
        for (ProducerAck ack : response.getMessagePublishResults()) {
            Assert.assertEquals(ack.getErrorCode(), 0, ack.getErrorMsg());
            String[] messageId = ack.getMessageId().split(":");
            long entryId = Long.parseLong(messageId[1]);
            int partition = Integer.parseInt(messageId[2]);
            Long lastEntryId = lastEntryIdByPartition.put(partition, entryId);
            Assert.assertTrue(lastEntryId == null || lastEntryId < entryId, ack.getMessageId());
        }
        Assert.assertEquals(lastEntryIdByPartition.size(), 3);
    }

    @Test
    public void testProduceWithBacklogQuotaTimeExceeded() throws Exception {
        getConfig().setPreciseTimeBasedBacklogQuotaCheck(true);
        String namespaceName = testTenant + "/" + testNamespace;
        String topicName = "persistent://" + namespaceName + "/" + testTopicName;
        admin.topics().createNonPartitionedTopic(topicName);
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build();
        admin.namespaces().setBacklogQuota(namespaceName, backlogQuota,
                BacklogQuota.BacklogQuotaType.message_age);
        admin.topics().createSubscription(topicName, "time-quota-sub", MessageId.earliest);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();
        producer.send("backlog-message");
        Topic topic = getTopic(topicName, false)
                .get()
                .orElseThrow(() -> new IllegalStateException("Topic not loaded: " + topicName));
        PersistentTopic persistentTopic = (PersistentTopic) topic;
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertTrue(persistentTopic.checkTimeBacklogExceeded(true).get()));

        AsyncResponse asyncResponse = mock(AsyncResponse.class);
        ProducerMessages producerMessages = new ProducerMessages();
        String message = "["
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1},"
                + "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}]";
        producerMessages.setMessages(createMessages(message));
        topics.produceOnPersistentTopic(asyncResponse, testTenant, testNamespace, testTopicName,
                false, producerMessages);
        ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
        verify(asyncResponse, timeout(5000).times(1)).resume(responseCaptor.capture());
        Assert.assertEquals(responseCaptor.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Object responseEntity = responseCaptor.getValue().getEntity();
        Assert.assertTrue(responseEntity instanceof ProducerAcks);
        ProducerAcks response = (ProducerAcks) responseEntity;
        Assert.assertEquals(response.getMessagePublishResults().size(), 2);
        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 2);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getErrorMsg()
                    .contains("Cannot create producer on topic with backlog quota exceeded"));
        }
    }

}
