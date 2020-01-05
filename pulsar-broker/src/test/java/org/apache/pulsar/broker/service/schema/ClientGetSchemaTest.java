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
package org.apache.pulsar.broker.service.schema;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import lombok.Cleanup;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClientGetSchemaTest extends ProducerConsumerBase {

    private static final String topicBytes = "my-property/my-ns/topic-bytes";
    private static final String topicString = "my-property/my-ns/topic-string";
    private static final String topicJson = "my-property/my-ns/topic-json";
    private static final String topicAvro = "my-property/my-ns/topic-avro";
    private static final String topicJsonNotNull = "my-property/my-ns/topic-json-not-null";
    private static final String topicAvroNotNull = "my-property/my-ns/topic-avro-not-null";

    List<Producer<?>> producers = new ArrayList<>();

    private static class MyClass {
        public String name;
        public int age;
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        // Create few topics with different types
        producers.add(pulsarClient.newProducer(Schema.BYTES).topic(topicBytes).create());
        producers.add(pulsarClient.newProducer(Schema.STRING).topic(topicString).create());
        producers.add(pulsarClient.newProducer(Schema.AVRO(MyClass.class)).topic(topicAvro).create());
        producers.add(pulsarClient.newProducer(Schema.JSON(MyClass.class)).topic(topicJson).create());
        producers.add(pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<MyClass>builder().withPojo(MyClass.class).build())).topic(topicAvro).create());
        producers.add(pulsarClient.newProducer(Schema.JSON(SchemaDefinition.<MyClass>builder().withPojo(MyClass.class).build())).topic(topicJson).create());
        producers.add(pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<MyClass>builder().withPojo(MyClass.class).withAlwaysAllowNull(false).build())).topic(topicAvroNotNull).create());
        producers.add(pulsarClient.newProducer(Schema.JSON(SchemaDefinition.<MyClass>builder().withPojo(MyClass.class).withAlwaysAllowNull(false).build())).topic(topicJsonNotNull).create());

    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        producers.forEach(t -> {
            try {
                t.close();
            } catch (PulsarClientException e) {
            }
        });
        super.internalCleanup();
    }

    @DataProvider(name = "serviceUrl")
    public String[] serviceUrls() {
        return new String[] {
                pulsar.getBrokerServiceUrl(),
                pulsar.getWebServiceAddress()
        };
    }

    @Test(dataProvider = "serviceUrl")
    public void testGetSchema(String serviceUrl) throws Exception {
        @Cleanup
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder().serviceUrl(serviceUrl).build();

        assertEquals(client.getSchema("non-existing-topic").join(), Optional.empty());
        assertEquals(client.getSchema(topicBytes).join(), Optional.empty());
        assertEquals(client.getSchema(topicString).join(), Optional.of(Schema.STRING.getSchemaInfo()));
        assertEquals(client.getSchema(topicJson).join(), Optional.of(Schema.JSON(MyClass.class).getSchemaInfo()));
        assertEquals(client.getSchema(topicAvro).join(), Optional.of(Schema.AVRO(MyClass.class).getSchemaInfo()));
    }

}
