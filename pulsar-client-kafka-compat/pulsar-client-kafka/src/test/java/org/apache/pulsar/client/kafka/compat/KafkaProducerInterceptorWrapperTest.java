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
package org.apache.pulsar.client.kafka.compat;

import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.ProducerInterceptor;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptorWrapper;
import org.apache.pulsar.client.impl.ProducerInterceptors;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Random;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

public class KafkaProducerInterceptorWrapperTest {

    /**
     * This test case is to make sure information is not lost during process of convert Pulsar message to Kafka record
     * and back to Pulsar message.
     */
    @Test
    public void testProducerInterceptorConvertRecordCorrectly() {

        String topic = "topic name";
        int partitionID = 666;
        long timeStamp = Math.abs(new Random().nextLong());

        org.apache.kafka.clients.producer.ProducerInterceptor<String, byte[]> mockInterceptor1 =
                (org.apache.kafka.clients.producer.ProducerInterceptor<String, byte[]>) mock(org.apache.kafka.clients.producer.ProducerInterceptor.class);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ProducerRecord<String, byte[]> record = (ProducerRecord<String, byte[]>) invocation.getArguments()[0];
                assertEquals(record.key(), "original key");
                assertEquals(record.value(), "original value".getBytes());
                assertEquals(record.timestamp().longValue(), timeStamp);
                assertEquals(record.partition().intValue(), partitionID);
                return new ProducerRecord<String, byte[]>(topic, "processed key", "processed value".getBytes());
            }
        }).when(mockInterceptor1).onSend(any(ProducerRecord.class));

        org.apache.kafka.clients.producer.ProducerInterceptor<String, String> mockInterceptor2 =
                (org.apache.kafka.clients.producer.ProducerInterceptor<String, String>) mock(org.apache.kafka.clients.producer.ProducerInterceptor.class);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ProducerRecord<String, byte[]> record = (ProducerRecord<String, byte[]>) invocation.getArguments()[0];
                assertEquals(record.key(), "processed key");
                assertEquals(record.value(), "processed value".getBytes());
                assertEquals(record.timestamp().longValue(), timeStamp);
                assertEquals(record.partition().intValue(), partitionID);
                return record;
            }
        }).when(mockInterceptor2).onSend(any(ProducerRecord.class));


        Schema<String> pulsarKeySerializeSchema = new PulsarKafkaSchema<>(new StringSerializer());
        Schema<byte[]> pulsarValueSerializeSchema = new PulsarKafkaSchema<>(new ByteArraySerializer());
        ProducerInterceptors producerInterceptors = new ProducerInterceptors(
                Arrays.stream(new ProducerInterceptor[]{
                        new KafkaProducerInterceptorWrapper(mockInterceptor1, pulsarKeySerializeSchema, pulsarValueSerializeSchema, topic),
                        new KafkaProducerInterceptorWrapper(mockInterceptor2, pulsarKeySerializeSchema, pulsarValueSerializeSchema, topic)}).map(
                        ProducerInterceptorWrapper::new).collect(Collectors.toList()));

        TypedMessageBuilderImpl typedMessageBuilder = new TypedMessageBuilderImpl(null, new BytesSchema());
        typedMessageBuilder.key("original key");
        typedMessageBuilder.value("original value".getBytes());
        typedMessageBuilder.eventTime(timeStamp);
        typedMessageBuilder.property(KafkaMessageRouter.PARTITION_ID, String.valueOf(partitionID));
        typedMessageBuilder.getMessage();

        producerInterceptors.beforeSend(null, typedMessageBuilder.getMessage());

        verify(mockInterceptor1, times(1)).onSend(any(ProducerRecord.class));
        verify(mockInterceptor2, times(1)).onSend(any(ProducerRecord.class));
    }

    @DataProvider(name = "serializers")
    public Object[][] serializers() {
        return new Object[][] {
            {
                new StringSerializer(), StringDeserializer.class
            },
            {
                new LongSerializer(), LongDeserializer.class
            },
            {
                new IntegerSerializer(), IntegerDeserializer.class,
            },
            {
                new DoubleSerializer(), DoubleDeserializer.class,
            },
            {
                new BytesSerializer(), BytesDeserializer.class
            },
            {
                new ByteBufferSerializer(), ByteBufferDeserializer.class
            },
            {
                new ByteArraySerializer(), ByteArrayDeserializer.class
            }
        };
    }

    @Test(dataProvider = "serializers")
    public void testGetDeserializer(Serializer serializer, Class deserializerClass) {
        Deserializer deserializer = KafkaProducerInterceptorWrapper.getDeserializer(serializer);
        assertEquals(deserializer.getClass(), deserializerClass);
    }

}
