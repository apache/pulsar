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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.ProducerInterceptor;
import org.apache.pulsar.client.impl.ProducerInterceptors;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Random;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
                Assert.assertEquals(record.key(), "original key");
                Assert.assertEquals(record.value(), "original value".getBytes());
                Assert.assertEquals(record.timestamp().longValue(), timeStamp);
                Assert.assertEquals(record.partition().intValue(), partitionID);
                return new ProducerRecord<String, byte[]>(topic, "processed key", "processed value".getBytes());
            }
        }).when(mockInterceptor1).onSend(any(ProducerRecord.class));

        org.apache.kafka.clients.producer.ProducerInterceptor<String, String> mockInterceptor2 =
                (org.apache.kafka.clients.producer.ProducerInterceptor<String, String>) mock(org.apache.kafka.clients.producer.ProducerInterceptor.class);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ProducerRecord<String, byte[]> record = (ProducerRecord<String, byte[]>) invocation.getArguments()[0];
                Assert.assertEquals(record.key(), "processed key");
                Assert.assertEquals(record.value(), "processed value".getBytes());
                Assert.assertEquals(record.timestamp().longValue(), timeStamp);
                Assert.assertEquals(record.partition().intValue(), partitionID);
                return record;
            }
        }).when(mockInterceptor2).onSend(any(ProducerRecord.class));

        ProducerInterceptors producerInterceptors = new ProducerInterceptors(Arrays.asList(new ProducerInterceptor[]{
                new KafkaProducerInterceptorWrapper(mockInterceptor1, new StringSerializer(), new ByteArraySerializer(), topic),
                new KafkaProducerInterceptorWrapper(mockInterceptor2, new StringSerializer(), new ByteArraySerializer(), topic)}));

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

}
