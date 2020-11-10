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
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.common.api.proto.PulsarApi;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Consumer tag filtering tests.
 */
@Test
public class ConsumerFilterTest {

    private ByteBuf nullByteBuf = null;

    @Test(timeOut = 30000)
    public void testNullFilter() throws Exception {
        ConsumerFilter consumerFilter = new ConsumerNullFilter();
        List<PulsarApi.KeyValue> messageProperties = new ArrayList<>();

        // Anything should match with no filter.
        boolean match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertTrue(match);
    }

    @Test(timeOut = 30000)
    public void testAnyTag() throws Exception {
        ConsumerFilter consumerFilter = new ConsumerTagFilter();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("anytag0", "abc");
        consumerFilter.initFiltering(metadata);

        List<PulsarApi.KeyValue> messageProperties = new ArrayList<>();

        // No tags should not match.
        boolean match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertFalse(match);

        // Wrong tag should not match.
        PulsarApi.KeyValue prop1 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag0")
                .setValue("123")
                .build();
        messageProperties.add(prop1);

        match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertFalse(match);

        messageProperties.clear();

        // This tag should match.
        PulsarApi.KeyValue prop2 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag1")
                .setValue("abc")
                .build();
        messageProperties.add(prop2);

        match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertTrue(match);

        messageProperties.clear();

        // One of 3 tag should match.
        PulsarApi.KeyValue prop3 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag2")
                .setValue("123")
                .build();

        messageProperties.add(prop3);
        messageProperties.add(prop2);
        messageProperties.add(prop1);

        match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertTrue(match);
    }

    @Test(timeOut = 30000)
    public void testAllTag() throws Exception {
        ConsumerFilter consumerFilter = new ConsumerTagFilter();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("alltag0", "abc");
        consumerFilter.initFiltering(metadata);

        List<PulsarApi.KeyValue> messageProperties = new ArrayList<>();

        // No tags should not match.
        boolean match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertFalse(match);

        // Wrong tag should not match.
        PulsarApi.KeyValue prop1 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag0")
                .setValue("123")
                .build();
        messageProperties.add(prop1);

        match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertFalse(match);

        messageProperties.clear();

        // This tag should match.
        PulsarApi.KeyValue prop2 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag0")
                .setValue("abc")
                .build();
        messageProperties.add(prop2);

        match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertTrue(match);
    }

    @Test(timeOut = 30000)
    public void testAllTags() throws Exception {
        ConsumerFilter consumerFilter = new ConsumerTagFilter();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("alltag0", "abc");
        metadata.put("alltag1", "xyz");
        consumerFilter.initFiltering(metadata);

        List<PulsarApi.KeyValue> messageProperties = new ArrayList<>();

        // One tag should not match.
        PulsarApi.KeyValue prop1 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag0")
                .setValue("abc")
                .build();
        messageProperties.add(prop1);

        boolean match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertFalse(match);

        // Two tags that should match.
        PulsarApi.KeyValue prop2 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag1")
                .setValue("xyz")
                .build();
        messageProperties.add(prop2);

        match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertTrue(match);
    }

    @Test(timeOut = 30000)
    public void testAnyAllTags() throws Exception {
        ConsumerFilter consumerFilter = new ConsumerTagFilter();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("anytag0", "123");
        metadata.put("alltag0", "abc");
        metadata.put("alltag1", "xyz");
        consumerFilter.initFiltering(metadata);

        List<PulsarApi.KeyValue> messageProperties = new ArrayList<>();

        // One tag should not match.
        PulsarApi.KeyValue prop1 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag0")
                .setValue("123")
                .build();
        messageProperties.add(prop1);

        boolean match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertFalse(match);

        // Two tags that should not match.
        PulsarApi.KeyValue prop2 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag1")
                .setValue("abc")
                .build();
        messageProperties.add(prop2);

        match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertFalse(match);

        // Three tags that should match.
        PulsarApi.KeyValue prop3 = PulsarApi.KeyValue.newBuilder()
                .setKey("tag1")
                .setValue("xyz")
                .build();
        messageProperties.add(prop3);

        match = consumerFilter.filter(messageProperties, nullByteBuf);
        assertTrue(match);
    }
}

