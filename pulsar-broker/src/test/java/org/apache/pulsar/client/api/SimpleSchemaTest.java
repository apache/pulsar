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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.client.api.PulsarClientException.IncompatibleSchemaException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class SimpleSchemaTest extends ProducerConsumerBase {

    @DataProvider(name = "schemaValidationModes")
    public static Object[][] schemaValidationModes() {
        return new Object[][] { { true }, { false } };
    }

    private final boolean schemaValidationEnforced;

    @Factory(dataProvider = "schemaValidationModes")
    public SimpleSchemaTest(boolean schemaValidationEnforced) {
        this.schemaValidationEnforced = schemaValidationEnforced;
    }


    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setSchemaValidationEnforced(schemaValidationEnforced);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testString() throws Exception {
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1").create()) {
            int N = 10;

            for (int i = 0; i < N; i++) {
                producer.send("my-message-" + i);
            }

            for (int i = 0; i < N; i++) {
                Message<String> msg = consumer.receive();
                assertEquals(msg.getValue(), "my-message-" + i);

                consumer.acknowledge(msg);
            }
        }
    }

    static class V1Data {
        int i;

        V1Data() {
            this.i = 0;
        }

        V1Data(int i) {
            this.i = i;
        }

        @Override
        public int hashCode() {
            return i;
        }

        @Override
        public boolean equals(Object other) {
            return (other instanceof V1Data) && i == ((V1Data)other).i;
        }
    }

    @Test
    public void newProducerNewTopicNewSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";
        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }
    }

    @Test
    public void newProducerTopicExistsWithoutSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";
        try (Producer<byte[]> p = pulsarClient.newProducer().topic(topic).create()) {
            p.send(topic.getBytes(UTF_8));
        }

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }
    }

    @Test
    public void newProducerTopicExistsWithSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";
        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(1));
        }

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }
    }

    @Test
    public void newProducerWithoutSchemaOnTopicWithSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }

        try (Producer<byte[]> p = pulsarClient.newProducer(Schema.BYTES).topic(topic).create()) {
            if (!schemaValidationEnforced) {
                p.send("junkdata".getBytes(UTF_8));
            } else {
                Assert.fail("Shouldn't be able to connect to a schema'd topic with no schema"
                    + " if SchemaValidationEnabled is enabled");
            }
        } catch (PulsarClientException e) {
            if (schemaValidationEnforced) {
                Assert.assertTrue(e instanceof IncompatibleSchemaException);
            } else {
                Assert.fail("Shouldn't throw IncompatibleSchemaException"
                    + " if SchemaValidationEnforced is disabled");
            }
        }

        // if using AUTO_PRODUCE_BYTES, producer can connect but the publish will fail
        try (Producer<byte[]> p = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES()).topic(topic).create()) {
            p.send("junkdata".getBytes(UTF_8));
        } catch (PulsarClientException e) {
            assertTrue(e.getCause() instanceof SchemaSerializationException);
        }
    }

    @Test
    public void newConsumerWithSchemaOnNewTopic() throws Exception {
        String topic = "my-property/my-ns/schema-test";

        try (Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
                .topic(topic).subscriptionName("sub1").subscribe();
             Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topic).create()) {
            V1Data toSend = new V1Data(1);
            p.send(toSend);
            Assert.assertEquals(toSend, c.receive().getValue());
        }
    }

    @Test
    public void newConsumerWithSchemaOnExistingTopicWithoutSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";

        try (Producer<byte[]> p = pulsarClient.newProducer().topic(topic).create();
             Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
                .topic(topic).subscriptionName("sub1").subscribe()) {
            Assert.fail("Shouldn't be able to consume with a schema from a topic which has no schema set");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof IncompatibleSchemaException);
        }
    }

    @Test
    public void newConsumerWithSchemaTopicHasSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topic).create();
             Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
                .topic(topic).subscriptionName("sub1").subscribe()) {
            V1Data toSend = new V1Data(1);
            p.send(toSend);
            Assert.assertEquals(toSend, c.receive().getValue());
        }
    }

    @Test
    public void newBytesConsumerWithTopicWithSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topic).create();
             Consumer<byte[]> c = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe()) {
            p.send(new V1Data(1));
            Assert.assertTrue(c.receive().getValue().length > 0);
        }
    }

    @Test
    public void getSchemaVersionFromMessagesBatchingDisabled() throws Exception {
        getSchemaVersionFromMessages(false);
    }

    @Test
    public void getSchemaVersionFromMessagesBatchingEnabled() throws Exception {
        getSchemaVersionFromMessages(true);
    }

    private void getSchemaVersionFromMessages(boolean batching) throws Exception {
        String topic = "my-property/my-ns/schema-test";

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
             .topic(topic)
             .enableBatching(batching)
             .create();
             Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
             .topic(topic)
             .subscriptionName("sub1")
             .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
             .subscribe()) {

            p.send(new V1Data(1));
            Message<V1Data> data = c.receive();
            assertNotNull(data.getSchemaVersion());
            assertEquals(data.getValue(), new V1Data(1));
        }
    }
}
