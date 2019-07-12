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
package org.apache.pulsar.client.kafka.compat.examples;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.kafka.compat.examples.utils.Bar;
import org.apache.pulsar.client.kafka.compat.examples.utils.Foo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerAvroExample {

    public static void main(String[] args) {
        String topic = "persistent://public/default/test-avro";

        Properties props = new Properties();
        props.put("bootstrap.servers", "pulsar://localhost:6650");
        props.put("group.id", "my-subscription-name");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", IntegerDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);

        @SuppressWarnings("resource")
        Consumer<Foo, Bar> consumer = new KafkaConsumer<>(props, fooSchema, barSchema);
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<Foo, Bar> records = consumer.poll(100);
            records.forEach(record -> {
                log.info("Received record: {}", record);
            });

            // Commit last offset
            consumer.commitSync();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerExample.class);
}
