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
package org.apache.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.testng.annotations.Test;

public class DebeziumTest {

//    @Test
    private void testJsonConverterBytes() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

        Schema<KeyValue<byte[], byte[]>> schema =
                Schema.KeyValue(Schema.BYTES, Schema.BYTES, KeyValueEncodingType.SEPARATED);

        Consumer<KeyValue<byte[], byte[]>> consumer = pulsarClient.newConsumer(schema)
                .topic("public/default/dbserver1.inventory.products")
                .subscriptionName("journey-test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        while (true) {
            Message<KeyValue<byte[], byte[]>> message = consumer.receive();
            KeyValue<byte[], byte[]> keyValue = message.getValue();
            System.out.println("----------- get message -----------");
            System.out.println("key: " + new String(keyValue.getKey()));
            System.out.println("value: " + new String(keyValue.getValue()));
        }
    }

//    @Test
    private void testJsonConverter() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

        Schema<KeyValue<GenericRecord, GenericRecord>> schema =
                Schema.KeyValue(Schema.AUTO_CONSUME(), Schema.AUTO_CONSUME(), KeyValueEncodingType.SEPARATED);

        Consumer<KeyValue<GenericRecord, GenericRecord>> consumer = pulsarClient.newConsumer(schema)
                .topic("public/default/dbserver1.inventory.products")
                .subscriptionName("journey-test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        while (true) {
            Message<KeyValue<GenericRecord, GenericRecord>> message = consumer.receive();
            KeyValue<GenericRecord, GenericRecord> keyValue = message.getValue();
            System.out.println("----------- get message -----------");
            System.out.println("key: " + new String(message.getKeyBytes()));
            System.out.println("value: " + new String(message.getData()));
        }
    }

//    @Test
    private void testAvroConverter() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

        Schema<KeyValue<GenericRecord, GenericRecord>> schema =
                Schema.KeyValue(Schema.AUTO_CONSUME(), Schema.AUTO_CONSUME(), KeyValueEncodingType.SEPARATED);

        Consumer<KeyValue<GenericRecord, GenericRecord>> consumer = pulsarClient.newConsumer(schema)
                .topic("public/default/dbserver1.inventory.products")
                .subscriptionName("journey-test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        while (true) {
            Message<KeyValue<GenericRecord, GenericRecord>> message = consumer.receive();
            try {
                message.getKeyBytes();
                message.getData();
                KeyValue<GenericRecord, GenericRecord> result = message.getValue();
                System.out.println("------------- got message -------------");

                System.out.println("key >>>>>>>>>>> ");
                for (Field field : result.getKey().getFields()) {
                    Object obj = result.getKey().getField(field);
                    System.out.println(field.getName() + ":" + (obj == null ? "null" : obj.toString()));
                }

                System.out.println("value >>>>>>>>>>> ");
                for (Field field : result.getValue().getFields()) {
                    Object obj = result.getValue().getField(field);
                    System.out.println(field.getName() + ":" + (obj == null ? "null" : obj.toString()));
                    if (obj != null && !"null".equalsIgnoreCase(obj.toString()) && (field.getName().equals("source") ||
                            field.getName().equals("before") || field.getName().equals("after"))) {
                        for (Field innerField : ((GenericRecord) obj).getFields()) {
                            Object innerObj = ((GenericRecord) obj).getField(innerField);
                            System.out.println("    " + innerField.getName() + ":" + (innerObj == null ? "null" : innerObj.toString()));
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
//                consumer.acknowledge(message);
            }
        }
    }
}
