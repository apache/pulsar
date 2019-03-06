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
package org.apache.pulsar.client.api;

import static org.testng.Assert.fail;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import java.time.Clock;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.schema.SchemaCompatibilityStrategy;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SimpleTypedProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(SimpleTypedProducerConsumerTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testJsonProducerAndConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        JSONSchema<JsonEncodedPojo> jsonSchema =
            JSONSchema.of(JsonEncodedPojo.class, true);

        Consumer<JsonEncodedPojo> consumer = pulsarClient
            .newConsumer(jsonSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();

        Producer<JsonEncodedPojo> producer = pulsarClient
            .newProducer(jsonSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(new JsonEncodedPojo(message));
        }

        Message<JsonEncodedPojo> msg = null;
        Set<JsonEncodedPojo> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            JsonEncodedPojo receivedMessage = msg.getValue();
            log.debug("Received message: [{}]", receivedMessage);
            JsonEncodedPojo expectedMessage = new JsonEncodedPojo("my-message-" + i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
            .getSchema("my-property/my-ns/my-topic1")
            .get();

        Assert.assertEquals(storedSchema.schema.getData(), jsonSchema.getSchemaInfo().getSchema());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testJsonProducerAndConsumerWithPrestoredSchema() throws Exception {
        log.info("-- Starting {} test --", methodName);

        JSONSchema<JsonEncodedPojo> jsonSchema =
            JSONSchema.of(JsonEncodedPojo.class, true);

        pulsar.getSchemaRegistryService()
            .putSchemaIfAbsent("my-property/my-ns/my-topic1",
                SchemaData.builder()
                    .type(SchemaType.JSON)
                    .isDeleted(false)
                    .timestamp(Clock.systemUTC().millis())
                    .user("me")
                    .data(jsonSchema.getSchemaInfo().getSchema())
                    .props(Collections.emptyMap())
                    .build(),
                SchemaCompatibilityStrategy.FULL
            ).get();

        Consumer<JsonEncodedPojo> consumer = pulsarClient
            .newConsumer(jsonSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();

        Producer<JsonEncodedPojo> producer = pulsarClient
            .newProducer(jsonSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create();

        consumer.close();
        producer.close();

        SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
            .getSchema("my-property/my-ns/my-topic1")
            .get();

        Assert.assertEquals(storedSchema.schema.getData(), jsonSchema.getSchemaInfo().getSchema());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testJsonConsumerWithWrongCorruptedSchema() throws Exception {
        log.info("-- Starting {} test --", methodName);

        byte[] randomSchemaBytes = "hello".getBytes();

        pulsar.getSchemaRegistryService()
            .putSchemaIfAbsent("my-property/my-ns/my-topic1",
                SchemaData.builder()
                    .type(SchemaType.JSON)
                    .isDeleted(false)
                    .timestamp(Clock.systemUTC().millis())
                    .user("me")
                    .data(randomSchemaBytes)
                    .props(Collections.emptyMap())
                    .build(),
                SchemaCompatibilityStrategy.FULL
            ).get();

        Consumer<JsonEncodedPojo> consumer = pulsarClient
            .newConsumer(JSONSchema.of(JsonEncodedPojo.class, true))
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testJsonProducerWithWrongCorruptedSchema() throws Exception {
        log.info("-- Starting {} test --", methodName);

        byte[] randomSchemaBytes = "hello".getBytes();

        pulsar.getSchemaRegistryService()
            .putSchemaIfAbsent("my-property/my-ns/my-topic1",
                SchemaData.builder()
                    .type(SchemaType.JSON)
                    .isDeleted(false)
                    .timestamp(Clock.systemUTC().millis())
                    .user("me")
                    .data(randomSchemaBytes)
                    .props(Collections.emptyMap())
                    .build(),
                SchemaCompatibilityStrategy.FULL
            ).get();

        Producer<JsonEncodedPojo> producer = pulsarClient
            .newProducer(JSONSchema.of(JsonEncodedPojo.class, true))
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create();


        log.info("-- Exiting {} test --", methodName);
    }


    @Test
    public void testProtobufProducerAndConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        ProtobufSchema<org.apache.pulsar.client.api.schema.proto.Test.TestMessage> protobufSchema =
                ProtobufSchema.of(org.apache.pulsar.client.api.schema.proto.Test.TestMessage.class);

        Consumer<org.apache.pulsar.client.api.schema.proto.Test.TestMessage> consumer = pulsarClient
                .newConsumer(protobufSchema)
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name")
                .subscribe();

        Producer<org.apache.pulsar.client.api.schema.proto.Test.TestMessage> producer = pulsarClient
                .newProducer(protobufSchema)
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .create();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(org.apache.pulsar.client.api.schema.proto.Test.TestMessage.newBuilder()
                    .setStringField(message).build());
        }

        Message<org.apache.pulsar.client.api.schema.proto.Test.TestMessage> msg = null;
        Set<org.apache.pulsar.client.api.schema.proto.Test.TestMessage> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            org.apache.pulsar.client.api.schema.proto.Test.TestMessage receivedMessage = msg.getValue();
            log.debug("Received message: [{}]", receivedMessage);
            org.apache.pulsar.client.api.schema.proto.Test.TestMessage expectedMessage
                    = org.apache.pulsar.client.api.schema.proto.Test.TestMessage.newBuilder()
                    .setStringField("my-message-" + i).build();

            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
                .getSchema("my-property/my-ns/my-topic1")
                .get();

        Assert.assertEquals(storedSchema.schema.getData(), protobufSchema.getSchemaInfo().getSchema());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(expectedExceptions = {PulsarClientException.class})
    public void testProtobufConsumerWithWrongPrestoredSchema() throws Exception {
        log.info("-- Starting {} test --", methodName);

        ProtobufSchema<org.apache.pulsar.client.api.schema.proto.Test.TestMessage> schema
                = ProtobufSchema.of(org.apache.pulsar.client.api.schema.proto.Test.TestMessage.class);

        pulsar.getSchemaRegistryService()
                .putSchemaIfAbsent("my-property/my-ns/my-topic1",
                        SchemaData.builder()
                                .type(SchemaType.PROTOBUF)
                                .isDeleted(false)
                                .timestamp(Clock.systemUTC().millis())
                                .user("me")
                                .data(schema.getSchemaInfo().getSchema())
                                .props(Collections.emptyMap())
                                .build(),
                        SchemaCompatibilityStrategy.FULL
                ).get();

        Consumer<org.apache.pulsar.client.api.schema.proto.Test.TestMessageWrong> consumer = pulsarClient
                .newConsumer(AvroSchema.of(org.apache.pulsar.client.api.schema.proto.Test.TestMessageWrong.class, true))
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name")
                .subscribe();

        log.info("-- Exiting {} test --", methodName);
    }

   @Test
   public void testAvroProducerAndConsumer() throws Exception {
       log.info("-- Starting {} test --", methodName);

       AvroSchema<AvroEncodedPojo> avroSchema =
           AvroSchema.of(AvroEncodedPojo.class, true);

       Consumer<AvroEncodedPojo> consumer = pulsarClient
           .newConsumer(avroSchema)
           .topic("persistent://my-property/use/my-ns/my-topic1")
           .subscriptionName("my-subscriber-name")
           .subscribe();

       Producer<AvroEncodedPojo> producer = pulsarClient
           .newProducer(avroSchema)
           .topic("persistent://my-property/use/my-ns/my-topic1")
           .create();

       for (int i = 0; i < 10; i++) {
           String message = "my-message-" + i;
           producer.send(new AvroEncodedPojo(message));
       }

       Message<AvroEncodedPojo> msg = null;
       Set<AvroEncodedPojo> messageSet = Sets.newHashSet();
       for (int i = 0; i < 10; i++) {
           msg = consumer.receive(5, TimeUnit.SECONDS);
           AvroEncodedPojo receivedMessage = msg.getValue();
           log.debug("Received message: [{}]", receivedMessage);
           AvroEncodedPojo expectedMessage = new AvroEncodedPojo("my-message-" + i);
           testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
       }
       // Acknowledge the consumption of all messages at once
       consumer.acknowledgeCumulative(msg);
       consumer.close();

       SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
           .getSchema("my-property/my-ns/my-topic1")
           .get();

       Assert.assertEquals(storedSchema.schema.getData(), avroSchema.getSchemaInfo().getSchema());

       log.info("-- Exiting {} test --", methodName);

   }

    @Test(expectedExceptions = {PulsarClientException.class})
    public void testAvroConsumerWithWrongPrestoredSchema() throws Exception {
        log.info("-- Starting {} test --", methodName);

        byte[] randomSchemaBytes = ("{\n" +
            "     \"type\": \"record\",\n" +
            "     \"namespace\": \"com.example\",\n" +
            "     \"name\": \"FullName\",\n" +
            "     \"fields\": [\n" +
            "       { \"name\": \"first\", \"type\": \"string\" },\n" +
            "       { \"name\": \"last\", \"type\": \"string\" }\n" +
            "     ]\n" +
            "} ").getBytes();

        pulsar.getSchemaRegistryService()
            .putSchemaIfAbsent("my-property/my-ns/my-topic1",
                SchemaData.builder()
                    .type(SchemaType.AVRO)
                    .isDeleted(false)
                    .timestamp(Clock.systemUTC().millis())
                    .user("me")
                    .data(randomSchemaBytes)
                    .props(Collections.emptyMap())
                    .build(),
                SchemaCompatibilityStrategy.FULL
            ).get();

        Consumer<AvroEncodedPojo> consumer = pulsarClient
            .newConsumer(AvroSchema.of(AvroEncodedPojo.class, true))
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();

        log.info("-- Exiting {} test --", methodName);
    }

    public static class AvroEncodedPojo {
        private String message;

        public AvroEncodedPojo() {
        }

        public AvroEncodedPojo(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AvroEncodedPojo that = (AvroEncodedPojo) o;
            return Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("message", message)
                .toString();
        }
    }

    public static class JsonEncodedPojo {
        private String message;

        public JsonEncodedPojo() {
        }

        public JsonEncodedPojo(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JsonEncodedPojo that = (JsonEncodedPojo) o;
            return Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("message", message)
                .toString();
        }
    }

    @Test
    public void testAvroProducerAndAutoSchemaConsumer() throws Exception {
       log.info("-- Starting {} test --", methodName);

       AvroSchema<AvroEncodedPojo> avroSchema =
           AvroSchema.of(AvroEncodedPojo.class, true);

       Producer<AvroEncodedPojo> producer = pulsarClient
           .newProducer(avroSchema)
           .topic("persistent://my-property/use/my-ns/my-topic1")
           .create();

       for (int i = 0; i < 10; i++) {
           String message = "my-message-" + i;
           producer.send(new AvroEncodedPojo(message));
       }

       Consumer<GenericRecord> consumer = pulsarClient
           .newConsumer(Schema.AUTO_CONSUME())
           .topic("persistent://my-property/use/my-ns/my-topic1")
           .subscriptionName("my-subscriber-name")
           .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
           .subscribe();

       Message<GenericRecord> msg = null;
       Set<String> messageSet = Sets.newHashSet();
       for (int i = 0; i < 10; i++) {
           msg = consumer.receive(5, TimeUnit.SECONDS);
           GenericRecord receivedMessage = msg.getValue();
           log.debug("Received message: [{}]", receivedMessage);
           String expectedMessage = "my-message-" + i;
           String actualMessage = (String) receivedMessage.getField("message");
           testMessageOrderAndDuplicates(messageSet, actualMessage, expectedMessage);
       }
       // Acknowledge the consumption of all messages at once
       consumer.acknowledgeCumulative(msg);
       consumer.close();

       SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
           .getSchema("my-property/my-ns/my-topic1")
           .get();

       Assert.assertEquals(storedSchema.schema.getData(), avroSchema.getSchemaInfo().getSchema());

       log.info("-- Exiting {} test --", methodName);

   }

   @Test
    public void testAvroProducerAndAutoSchemaReader() throws Exception {
       log.info("-- Starting {} test --", methodName);

       AvroSchema<AvroEncodedPojo> avroSchema =
           AvroSchema.of(AvroEncodedPojo.class, true);

       Producer<AvroEncodedPojo> producer = pulsarClient
           .newProducer(avroSchema)
           .topic("persistent://my-property/use/my-ns/my-topic1")
           .create();

       for (int i = 0; i < 10; i++) {
           String message = "my-message-" + i;
           producer.send(new AvroEncodedPojo(message));
       }

       Reader<GenericRecord> reader = pulsarClient
           .newReader(Schema.AUTO_CONSUME())
           .topic("persistent://my-property/use/my-ns/my-topic1")
           .startMessageId(MessageId.earliest)
           .create();

       Message<GenericRecord> msg = null;
       Set<String> messageSet = Sets.newHashSet();
       for (int i = 0; i < 10; i++) {
           msg = reader.readNext();
           GenericRecord receivedMessage = msg.getValue();
           log.debug("Received message: [{}]", receivedMessage);
           String expectedMessage = "my-message-" + i;
           String actualMessage = (String) receivedMessage.getField("message");
           testMessageOrderAndDuplicates(messageSet, actualMessage, expectedMessage);
       }
       // Acknowledge the consumption of all messages at once
       reader.close();

       SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
           .getSchema("my-property/my-ns/my-topic1")
           .get();

       Assert.assertEquals(storedSchema.schema.getData(), avroSchema.getSchemaInfo().getSchema());

       log.info("-- Exiting {} test --", methodName);

   }

    @Test
    public void testAutoBytesProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        AvroSchema<AvroEncodedPojo> avroSchema =
            AvroSchema.of(AvroEncodedPojo.class, true);

        try (Producer<AvroEncodedPojo> producer = pulsarClient
            .newProducer(avroSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create()) {
            for (int i = 0; i < 10; i++) {
                String message = "my-message-" + i;
                producer.send(new AvroEncodedPojo(message));
            }
        }

        try (Producer<byte[]> producer = pulsarClient
            .newProducer(Schema.AUTO_PRODUCE_BYTES())
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create()) {
            // try to produce junk data
            for (int i = 10; i < 20; i++) {
                String message = "my-message-" + i;
                byte[] data = avroSchema.encode(new AvroEncodedPojo(message));
                byte[] junkData = new byte[data.length / 2];
                System.arraycopy(data, 0, junkData, 0, junkData.length);
                try {
                    producer.send(junkData);
                    fail("Should fail on sending junk data");
                } catch (SchemaSerializationException sse) {
                    // expected
                }
            }

            for (int i = 10; i < 20; i++) {
                String message = "my-message-" + i;
                byte[] data = avroSchema.encode(new AvroEncodedPojo(message));
                producer.send(data);
            }
        }

        Consumer<GenericRecord> consumer = pulsarClient
            .newConsumer(Schema.AUTO_CONSUME())
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();

        Message<GenericRecord> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 20; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            GenericRecord receivedMessage = msg.getValue();
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            String actualMessage = (String) receivedMessage.getField("message");
            testMessageOrderAndDuplicates(messageSet, actualMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
            .getSchema("my-property/my-ns/my-topic1")
            .get();

        Assert.assertEquals(storedSchema.schema.getData(), avroSchema.getSchemaInfo().getSchema());

        log.info("-- Exiting {} test --", methodName);

    }
}
