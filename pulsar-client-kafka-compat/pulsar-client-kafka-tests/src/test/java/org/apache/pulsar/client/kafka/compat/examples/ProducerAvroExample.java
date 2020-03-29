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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.kafka.compat.examples.utils.Bar;
import org.apache.pulsar.client.kafka.compat.examples.utils.Foo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAvroExample {
    public static void main(String[] args) {
        String topic = "persistent://public/default/test-avro";

        Properties props = new Properties();
        props.put("bootstrap.servers", "pulsar://localhost:6650");

        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);


        Producer<Foo, Bar> producer = new KafkaProducer<>(props, fooSchema, barSchema);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<Foo, Bar>(topic, i, foo, bar));
            log.info("Message {} sent successfully", i);
        }

        producer.flush();
        producer.close();
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerExample.class);
}
