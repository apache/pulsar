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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.time.Duration;
import java.util.Objects;
import lombok.Cleanup;
import org.apache.pulsar.client.api.schema.proto.Test.TestEnum;
import org.apache.pulsar.client.api.schema.proto.Test.TestMessage;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Roundtrip every {@code Schema.*} factory through the V5 producer / consumer wire path
 * on a single-segment scalable topic. One test per schema; each sends a sentinel value
 * and asserts the consumer reads it back unchanged.
 *
 * <p>{@code autoProduceBytes} requires a more elaborate setup (broker-side schema must
 * already be registered for the topic) and is covered separately under producer knobs.
 */
public class V5SchemaRoundtripTest extends V5ClientBaseTest {

    private <T> T roundtrip(Schema<T> schema, T value) throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<T> producer = v5Client.newProducer(schema)
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<T> consumer = v5Client.newQueueConsumer(schema)
                .topic(topic)
                .subscriptionName("schema-sub")
                .subscribe();

        producer.newMessage().value(value).send();
        Message<T> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "message did not arrive within 5s");
        T received = msg.value();
        consumer.acknowledge(msg.id());
        return received;
    }

    @Test
    public void testBytes() throws Exception {
        byte[] sent = "hello-bytes".getBytes();
        byte[] got = roundtrip(Schema.bytes(), sent);
        assertEquals(got, sent);
    }

    @Test
    public void testString() throws Exception {
        assertEquals(roundtrip(Schema.string(), "hello-string"), "hello-string");
    }

    @Test
    public void testBool() throws Exception {
        assertEquals(roundtrip(Schema.bool(), Boolean.TRUE), Boolean.TRUE);
    }

    @Test
    public void testInt8() throws Exception {
        assertEquals(roundtrip(Schema.int8(), (byte) -7), (byte) -7);
    }

    @Test
    public void testInt16() throws Exception {
        assertEquals(roundtrip(Schema.int16(), (short) 12345), (short) 12345);
    }

    @Test
    public void testInt32() throws Exception {
        assertEquals(roundtrip(Schema.int32(), 123_456_789), Integer.valueOf(123_456_789));
    }

    @Test
    public void testInt64() throws Exception {
        long v = 1234567890123L;
        assertEquals(roundtrip(Schema.int64(), v), Long.valueOf(v));
    }

    @Test
    public void testFloat32() throws Exception {
        assertEquals(roundtrip(Schema.float32(), 3.14f), 3.14f, 1e-6f);
    }

    @Test
    public void testFloat64() throws Exception {
        assertEquals(roundtrip(Schema.float64(), 2.718281828d), 2.718281828d, 1e-9);
    }

    @Test
    public void testJson() throws Exception {
        Pojo sent = new Pojo("alice", 30);
        Pojo got = roundtrip(Schema.json(Pojo.class), sent);
        assertEquals(got, sent);
    }

    @Test
    public void testAvro() throws Exception {
        Pojo sent = new Pojo("bob", 42);
        Pojo got = roundtrip(Schema.avro(Pojo.class), sent);
        assertEquals(got, sent);
    }

    @Test
    public void testProtobuf() throws Exception {
        TestMessage sent = TestMessage.newBuilder()
                .setStringField("proto-roundtrip")
                .setDoubleField(3.14159)
                .setIntField(42)
                .setTestEnum(TestEnum.SHARED)
                .build();
        TestMessage got = roundtrip(Schema.protobuf(TestMessage.class), sent);
        assertEquals(got.getStringField(), "proto-roundtrip");
        assertEquals(got.getDoubleField(), 3.14159, 1e-9);
        assertEquals(got.getIntField(), 42);
        assertEquals(got.getTestEnum(), TestEnum.SHARED);
    }

    /**
     * Tiny POJO for json/avro schema roundtrips. Public no-args + getters/setters
     * keep both Jackson (json) and Avro reflection happy.
     */
    public static class Pojo {
        private String name;
        private int age;

        public Pojo() {
        }

        public Pojo(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Pojo other)) {
                return false;
            }
            return age == other.age && Objects.equals(name, other.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }
    }
}
