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

package org.apache.pulsar.io.kafka;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.io.core.SourceContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

@Slf4j
public class ByteBufferSchemaWrapperTest {

    @Test
    public void testGetBytesNoCopy() throws Exception {
        byte[] originalArray = {1, 2, 3};
        ByteBuffer wrapped = ByteBuffer.wrap(originalArray);
        assertEquals(0, wrapped.arrayOffset());
        assertEquals(3, wrapped.remaining());
        assertSame(ByteBufferSchemaWrapper.getBytes(wrapped), originalArray);
    }

    @Test
    public void testGetBytesOffsetZeroDifferentLen() throws Exception {
        byte[] originalArray = {1, 2, 3};
        ByteBuffer wrapped = ByteBuffer.wrap(originalArray, 1, 2);
        assertEquals(0, wrapped.arrayOffset());
        assertEquals(2, wrapped.remaining());
        byte[] result = ByteBufferSchemaWrapper.getBytes(wrapped);
        assertNotSame(result, originalArray);
        assertArrayEquals(result, new byte[] {2,3});
    }

    @Test
    public void testGetBytesOffsetNonZero() throws Exception {
        byte[] originalArray = {1, 2, 3};
        ByteBuffer wrapped = ByteBuffer.wrap(originalArray);
        wrapped.position(1);
        assertEquals(1, wrapped.position());
        wrapped = wrapped.slice();
        assertEquals(1, wrapped.arrayOffset());
        assertEquals(2, wrapped.remaining());
        byte[] result = ByteBufferSchemaWrapper.getBytes(wrapped);
        assertNotSame(result, originalArray);
        assertArrayEquals(result, new byte[] {2,3});
    }

    @Test
    public void testGetBytesOffsetZero() throws Exception {
        byte[] originalArray = {1, 2, 3};
        ByteBuffer wrapped = ByteBuffer.wrap(originalArray, 0, 2);
        assertEquals(0, wrapped.arrayOffset());
        assertEquals(2, wrapped.remaining());
        byte[] result = ByteBufferSchemaWrapper.getBytes(wrapped);
        assertNotSame(result, originalArray);
        assertArrayEquals(result, new byte[] {1,2});
    }

}
