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
package org.apache.pulsar.broker.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
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
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.rest.base.RestClientImpl;
import org.apache.pulsar.broker.rest.base.api.RestClient;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAcks;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.mockito.Mockito.spy;

public class TopicsIntegrationTest extends MultiBrokerBaseTest {

    private Topics topics;
    private final String testLocalCluster = "test";
    private final String testTenant = "my-tenant";
    private final String testNamespace = "my-namespace";
    private final String testTopicName = "my-topic";

    private RestClient restClient;

    protected void pulsarResourcesSetup() throws PulsarAdminException {
        topics = spy(new Topics());
        topics.setPulsar(pulsar);
        admin.clusters().createCluster(testLocalCluster, new ClusterDataImpl());
        admin.tenants().createTenant(testTenant, new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                Sets.newHashSet(testLocalCluster)));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace,
                Sets.newHashSet(testLocalCluster));
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        clientConfigurationData.setServiceUrl(pulsar.getWebServiceAddress());
        try {
            restClient = new RestClientImpl(pulsar.getWebServiceAddress(), clientConfigurationData);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testProduceToNonPersistentTopic() throws Exception {
        String topicName = "non-persistent://" + testTenant + "/" + testNamespace + "/testProduceToNonPersistentTopic";
        admin.topics().createPartitionedTopic(topicName, 3);
        Schema keyValueSchema = KeyValueSchemaImpl.of(StringSchema.utf8(), StringSchema.utf8(),
                KeyValueEncodingType.SEPARATED);
        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(keyValueSchema)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "[" +
                "{\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        restClient.producer().send(topicName, producerMessages);
        for (int i = 0; i < 3; i ++) {
            long entries = admin.topics().getStats(TopicName.get(topicName).getPartition(i).toString()).getMsgInCounter();
            Assert.assertEquals(entries, 1);
        }

        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
    }

    @Test
    public void testProduceToNonPersistentTopicSpecificPartition() throws Exception {
        String topicName = "non-persistent://" + testTenant + "/" + testNamespace + "/testProduceToNonPersistentTopicSpecificPartition";
        admin.topics().createPartitionedTopic(topicName, 3);
        Schema keyValueSchema = KeyValueSchemaImpl.of(StringSchema.utf8(), StringSchema.utf8(),
                KeyValueEncodingType.SEPARATED);
        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(keyValueSchema)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
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
        restClient.producer().send(topicName, 2, producerMessages);

        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
    }

    @Test
    public void testProduceToNonPartitionedTopic() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceToNonPartitionedTopic";
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
        ProducerAcks response = restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(admin.topics().getInternalStats(topicName ).currentLedgerEntries, 3);

        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
    }

    @Test
    public void testProduceToPartitionedTopic() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceToPartitionedTopic";
        admin.topics().createPartitionedTopic(topicName, 5);
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "[" +
                "{\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}," +
                "{\"payload\":\"RestProducer:4\",\"eventTime\":1603045262772,\"sequenceId\":4}," +
                "{\"payload\":\"RestProducer:5\",\"eventTime\":1603045262772,\"sequenceId\":5}," +
                "{\"payload\":\"RestProducer:6\",\"eventTime\":1603045262772,\"sequenceId\":6}," +
                "{\"payload\":\"RestProducer:7\",\"eventTime\":1603045262772,\"sequenceId\":7}," +
                "{\"payload\":\"RestProducer:8\",\"eventTime\":1603045262772,\"sequenceId\":8}," +
                "{\"payload\":\"RestProducer:9\",\"eventTime\":1603045262772,\"sequenceId\":9}," +
                "{\"payload\":\"RestProducer:10\",\"eventTime\":1603045262772,\"sequenceId\":10}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));

        ProducerAcks response = restClient.producer().send(topicName, producerMessages);

        for (int index = 0; index < response.getMessagePublishResults().size(); index++) {
            Assert.assertEquals(response.getMessagePublishResults().get(index).getErrorCode(), 0);
            Assert.assertTrue(response.getMessagePublishResults().get(index).getMessageId().length() > 0);
        }
        for (int i = 0; i < 5; i ++) {
            long entries = admin.topics().getInternalStats(TopicName.get(topicName).getPartition(i).toString()).currentLedgerEntries;
            Assert.assertEquals(entries, 2);
        }
    }

    @Test
    public void testProduceToPartitionedTopicSpecificPartition() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceToPartitionedTopicSpecificPartition";
        admin.topics().createPartitionedTopic(topicName, 5);
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

        ProducerAcks response = restClient.producer().send(topicName, 2, producerMessages);
        Assert.assertEquals(response.getMessagePublishResults().size(), 4);
        Assert.assertEquals(response.getSchemaVersion(), 0);
        Assert.assertEquals(admin.topics().getInternalStats(TopicName.get(topicName).getPartition(1).toString()).currentLedgerEntries, 4);

    }

    @Test
    public void testProduceWithLongSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceWithLongSchema";
        admin.topics().createNonPartitionedTopic(topicName);
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

        ProducerAcks response = restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(response.getMessagePublishResults().size(), 5);
        Assert.assertEquals(response.getSchemaVersion(), 0);

        List<Long> expectedMsg = Arrays.asList(111111111111l, 222222222222l, 333333333333l, 444444444444l, 555555555555l);
        Message<Long> msg = null;
        // Assert all messages published by REST producer can be received by consumer in expected order.
        for (int i = 0; i < 5; i++) {
            msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertEquals(expectedMsg.get(i), Schema.INT64.decode(msg.getData()));
            Assert.assertEquals("my-key", msg.getKey());
        }
    }

    @Test
    public void testProduceNoSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceNoSchema";
        admin.topics().createNonPartitionedTopic(topicName);
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

        ProducerAcks response = restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(response.getMessagePublishResults().size(), 5);
        Assert.assertEquals(response.getSchemaVersion(), 0);

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

    @Test
    public void testProduceWithJsonSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceWithJsonSchema";
        admin.topics().createNonPartitionedTopic(topicName);
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

        ProducerAcks response = restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(response.getMessagePublishResults().size(), 2);
        Assert.assertEquals(response.getSchemaVersion(), 0);

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
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceWithAvroSchema";
        admin.topics().createNonPartitionedTopic(topicName);
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

        ProducerAcks response = restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(response.getMessagePublishResults().size(), 2);
        Assert.assertEquals(response.getSchemaVersion(), 0);

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
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceWithRestAndClientThenConsumeWithClient";
        admin.topics().createNonPartitionedTopic(topicName);
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

        ProducerAcks response = restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(response.getMessagePublishResults().size(), 3);
        Assert.assertEquals(response.getSchemaVersion(), 0);

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
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceWithRestThenConsumeWithClient";
        admin.topics().createNonPartitionedTopic(topicName);
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

        ProducerAcks response = restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(response.getMessagePublishResults().size(), 5);
        Assert.assertEquals(response.getSchemaVersion(), 0);
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

        response = restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(response.getMessagePublishResults().size(), 5);
        Assert.assertEquals(response.getSchemaVersion(), 0);

        List<String> expectedMsg = Arrays.asList("RestProducer:1", "RestProducer:2", "RestProducer:3",
                "RestProducer:4", "RestProducer:5", "RestProducer:6",
                "RestProducer:7", "RestProducer:8", "RestProducer:9",
                "RestProducer:10");
        // Assert all messages published by REST producer can be received by consumer in expected order.
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertEquals(expectedMsg.get(i), StringSchema.utf8().decode(msg.getData()));
            Assert.assertEquals("bXkta2V5", msg.getKey());
        }
    }

    @Test
    public void testProduceWithIncompatibleSchema() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceWithIncompatibleSchema";
        admin.topics().createNonPartitionedTopic(topicName);
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

        try {
            restClient.producer().send(topicName, producerMessages);
            Assert.fail("Should fail here");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("IncompatibleSchemaException"));
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
}
